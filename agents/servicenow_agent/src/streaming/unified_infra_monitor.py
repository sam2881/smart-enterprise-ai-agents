#!/usr/bin/env python3
"""
Unified Infrastructure Monitor

Single process that monitors multiple infrastructure sources and creates
ServiceNow incidents + Kafka events for failures. Replaces separate
airflow_dag_monitor.py and gcp_vm_monitor.py with a shared base class.

Architecture:
  InfrastructureMonitor
   ├── AirflowMonitor  → polls Airflow REST API for failed DAGs
   └── GCPVMMonitor    → polls GCP Compute API for stopped VMs
  Shared:
   ├── ServiceNow incident creation
   ├── Kafka event publishing
   └── Threaded polling loops with graceful shutdown
"""

import os
import sys
import json
import time
import signal
import threading
import requests
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

from requests.auth import HTTPBasicAuth
from kafka import KafkaProducer


# =============================================================================
# Configuration
# =============================================================================

SNOW_URL = os.getenv('SNOW_INSTANCE_URL', 'https://dev349960.service-now.com')
SNOW_USER = os.getenv('SNOW_USERNAME', 'admin')
SNOW_PASS = os.getenv('SNOW_PASSWORD', '')
KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092')

# Airflow
AIRFLOW_URL = os.getenv('AIRFLOW_URL', 'http://localhost:8083')
AIRFLOW_USERNAME = os.getenv('AIRFLOW_USERNAME', 'admin')
AIRFLOW_PASSWORD = os.getenv('AIRFLOW_PASSWORD', 'admin')
AIRFLOW_POLL_INTERVAL = int(os.getenv('AIRFLOW_POLL_INTERVAL', '60'))
AIRFLOW_LOOKBACK_MINUTES = int(os.getenv('AIRFLOW_LOOKBACK_MINUTES', '5'))

# GCP
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID', 'agent-ai-test-461120')
GCP_ZONES = os.getenv('GCP_ZONES', 'us-central1-a,us-central1-b,us-east1-b').split(',')
GCP_POLL_INTERVAL = int(os.getenv('GCP_POLL_INTERVAL', '60'))
GOOGLE_APPLICATION_CREDENTIALS = os.getenv(
    'GOOGLE_APPLICATION_CREDENTIALS',
    str(Path(__file__).parent.parent.parent.parent.parent / 'gcp-service-account-key.json')
)


# =============================================================================
# Base Monitor
# =============================================================================

class BaseMonitor(ABC):
    """Shared base for all infrastructure monitors."""

    def __init__(
        self,
        name: str,
        kafka_producer: Optional[KafkaProducer],
        snow_auth: HTTPBasicAuth,
        poll_interval: int = 60,
    ):
        self.name = name
        self.kafka_producer = kafka_producer
        self.snow_auth = snow_auth
        self.poll_interval = poll_interval
        self._stop_event = threading.Event()

    # -- Shared: ServiceNow --------------------------------------------------

    def create_servicenow_incident(self, incident_data: Dict[str, str]) -> Dict[str, Any]:
        """Create a ServiceNow incident. Returns {incident_id, sys_id, success}."""
        try:
            response = requests.post(
                f"{SNOW_URL}/api/now/table/incident",
                auth=self.snow_auth,
                json=incident_data,
                headers={'Content-Type': 'application/json', 'Accept': 'application/json'},
                timeout=30,
            )
            if response.status_code in [200, 201]:
                result = response.json().get('result', {})
                incident_id = result.get('number', 'Unknown')
                sys_id = result.get('sys_id', '')
                print(f"   [{self.name}] ServiceNow incident created: {incident_id}")
                return {'incident_id': incident_id, 'sys_id': sys_id, 'success': True}
            else:
                print(f"   [{self.name}] Failed to create incident: {response.status_code}")
                return {'success': False, 'error': response.text[:200]}
        except Exception as e:
            print(f"   [{self.name}] Error creating incident: {e}")
            return {'success': False, 'error': str(e)}

    def query_servicenow(self, query: str, fields: str, limit: int = 10) -> List[Dict]:
        """Query ServiceNow table/incident with given sysparm_query."""
        try:
            response = requests.get(
                f"{SNOW_URL}/api/now/table/incident",
                auth=self.snow_auth,
                params={
                    'sysparm_query': query,
                    'sysparm_fields': fields,
                    'sysparm_limit': limit,
                },
                headers={'Accept': 'application/json'},
                timeout=15,
            )
            if response.status_code == 200:
                return response.json().get('result', [])
        except Exception as e:
            print(f"   [{self.name}] ServiceNow query error: {e}")
        return []

    # -- Shared: Kafka --------------------------------------------------------

    def publish_to_kafka(self, topic: str, key: str, event: Dict[str, Any]):
        """Publish an event to a Kafka topic."""
        if not self.kafka_producer:
            return
        try:
            future = self.kafka_producer.send(topic, key=key, value=event)
            meta = future.get(timeout=10)
            print(f"   [{self.name}] Published to Kafka: {topic} (partition: {meta.partition})")
        except Exception as e:
            print(f"   [{self.name}] Kafka publish error: {e}")

    # -- Shared: Polling loop -------------------------------------------------

    def run_loop(self):
        """Run the monitor in a polling loop until stopped."""
        print(f"[{self.name}] Starting (interval: {self.poll_interval}s)")
        while not self._stop_event.is_set():
            try:
                self.check_and_process()
            except Exception as e:
                print(f"[{self.name}] Error in check cycle: {e}")
            self._stop_event.wait(self.poll_interval)
        print(f"[{self.name}] Stopped")

    def stop(self):
        self._stop_event.set()

    # -- Abstract: source-specific --------------------------------------------

    @abstractmethod
    def check_and_process(self):
        """Run one check cycle: detect failures, create incidents, publish events."""

    @abstractmethod
    def list_status(self):
        """Print current status of monitored resources."""


# =============================================================================
# Airflow Monitor
# =============================================================================

class AirflowMonitor(BaseMonitor):
    """Monitors Airflow DAG runs for failures."""

    def __init__(self, kafka_producer, snow_auth):
        super().__init__(
            name="Airflow",
            kafka_producer=kafka_producer,
            snow_auth=snow_auth,
            poll_interval=AIRFLOW_POLL_INTERVAL,
        )
        self.airflow_auth = HTTPBasicAuth(AIRFLOW_USERNAME, AIRFLOW_PASSWORD)
        self.processed_dag_runs: Dict[str, datetime] = {}

    def _get_failed_dag_runs(self) -> List[Dict]:
        """Get failed DAG runs from Airflow API within lookback window."""
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(minutes=AIRFLOW_LOOKBACK_MINUTES)
        try:
            response = requests.get(
                f"{AIRFLOW_URL}/api/v1/dags/~/dagRuns",
                auth=self.airflow_auth,
                params={
                    'state': 'failed',
                    'start_date_gte': start_time.isoformat() + 'Z',
                    'end_date_lte': end_time.isoformat() + 'Z',
                    'limit': 100,
                },
                headers={'Content-Type': 'application/json'},
                timeout=30,
            )
            if response.status_code == 200:
                runs = response.json().get('dag_runs', [])
                new_runs = []
                for run in runs:
                    run_key = f"{run['dag_id']}-{run['dag_run_id']}"
                    if run_key not in self.processed_dag_runs:
                        new_runs.append({
                            'dag_id': run['dag_id'],
                            'dag_run_id': run['dag_run_id'],
                            'execution_date': run.get('execution_date', ''),
                            'start_date': run.get('start_date', ''),
                            'end_date': run.get('end_date', ''),
                            'state': run['state'],
                            'external_trigger': run.get('external_trigger', False),
                            'conf': run.get('conf', {}),
                        })
                return new_runs
            elif response.status_code == 401:
                print(f"   [{self.name}] Airflow authentication failed")
            else:
                print(f"   [{self.name}] Airflow API error: {response.status_code}")
        except requests.exceptions.ConnectionError:
            print(f"   [{self.name}] Cannot connect to Airflow at {AIRFLOW_URL}")
        except Exception as e:
            print(f"   [{self.name}] Error fetching DAG runs: {e}")
        return []

    def _get_failed_tasks(self, dag_id: str, dag_run_id: str) -> List[Dict]:
        """Get failed task instances for a DAG run."""
        try:
            response = requests.get(
                f"{AIRFLOW_URL}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances",
                auth=self.airflow_auth,
                headers={'Content-Type': 'application/json'},
                timeout=30,
            )
            if response.status_code == 200:
                tasks = response.json().get('task_instances', [])
                return [
                    {
                        'task_id': t['task_id'],
                        'state': t['state'],
                        'start_date': t.get('start_date', ''),
                        'end_date': t.get('end_date', ''),
                        'try_number': t.get('try_number', 1),
                        'max_tries': t.get('max_tries', 1),
                    }
                    for t in tasks
                    if t['state'] == 'failed'
                ]
        except Exception as e:
            print(f"   [{self.name}] Error fetching task instances: {e}")
        return []

    def _get_task_logs(self, dag_id: str, dag_run_id: str, task_id: str, try_number: int = 1) -> str:
        """Get last 50 lines of task logs."""
        try:
            response = requests.get(
                f"{AIRFLOW_URL}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{try_number}",
                auth=self.airflow_auth,
                headers={'Accept': 'text/plain'},
                timeout=30,
            )
            if response.status_code == 200:
                lines = response.text.split('\n')
                return '\n'.join(lines[-50:])
        except Exception as e:
            print(f"   [{self.name}] Error fetching task logs: {e}")
        return "Logs not available"

    def check_and_process(self):
        print(f"[{self.name}] Scanning at {datetime.now().strftime('%H:%M:%S')}...")
        failed_runs = self._get_failed_dag_runs()

        if not failed_runs:
            print(f"   [{self.name}] No new DAG failures detected")
            return

        for run in failed_runs:
            run_key = f"{run['dag_id']}-{run['dag_run_id']}"
            print(f"   [{self.name}] Failed DAG: {run['dag_id']} (run: {run['dag_run_id']})")

            failed_tasks = self._get_failed_tasks(run['dag_id'], run['dag_run_id'])
            task_logs = "No failed tasks found"
            if failed_tasks:
                task_logs = self._get_task_logs(
                    run['dag_id'], run['dag_run_id'],
                    failed_tasks[0]['task_id'],
                    failed_tasks[0].get('try_number', 1),
                )

            # Build structured task failure summary
            task_summary_lines = []
            for i, task in enumerate(failed_tasks, 1):
                task_summary_lines.append(
                    f"  {i}. {task['task_id']} "
                    f"(attempt {task['try_number']}/{task['max_tries']})"
                    f"\n     Started: {task.get('start_date', 'N/A')}"
                    f" | Ended: {task.get('end_date', 'N/A')}"
                )
            task_summary = "\n".join(task_summary_lines) or "  No individual task failures captured."
            total_tasks = len(failed_tasks)

            # Severity escalation for multi-task failures
            priority = '1' if total_tasks > 2 else '2'
            severity_note = (
                "CRITICAL - Multiple tasks failed; likely a systemic issue "
                "(connectivity, config, or upstream data)."
                if total_tasks > 2
                else "Single task failure - may be transient or data-specific."
            )

            # Extract error hint from the last log lines
            error_hint = "No specific error pattern detected in logs."
            if task_logs and task_logs != "No failed tasks found":
                for line in reversed(task_logs.split('\n')):
                    low = line.lower()
                    if any(kw in low for kw in [
                        'error', 'exception', 'traceback', 'failed',
                        'timeout', 'refused', 'permission',
                    ]):
                        error_hint = line.strip()[:300]
                        break

            # Create ServiceNow incident with rich, structured description
            incident_result = self.create_servicenow_incident({
                'short_description': (
                    f"[Airflow] DAG '{run['dag_id']}' failed "
                    f"- {total_tasks} task(s) "
                    f"- {datetime.now().strftime('%Y-%m-%d %H:%M')}"
                ),
                'description': (
                    "====================================================\n"
                    "AIRFLOW DAG FAILURE - AUTO-DETECTED\n"
                    "====================================================\n"
                    "\n"
                    "SUMMARY\n"
                    f"  DAG:            {run['dag_id']}\n"
                    f"  Run ID:         {run['dag_run_id']}\n"
                    f"  Execution Date: {run['execution_date']}\n"
                    f"  Window:         {run['start_date']} -> {run['end_date']}\n"
                    f"  Failed Tasks:   {total_tasks}\n"
                    "\n"
                    "IMPACT ASSESSMENT\n"
                    f"  {severity_note}\n"
                    "\n"
                    "FAILED TASKS\n"
                    f"{task_summary}\n"
                    "\n"
                    "ROOT CAUSE HINT (from logs)\n"
                    f"  {error_hint}\n"
                    "\n"
                    "TASK LOGS (last 50 lines)\n"
                    "---\n"
                    f"{task_logs[:3000]}\n"
                    "---\n"
                    "\n"
                    "RECOMMENDED REMEDIATION\n"
                    "  1. Review the root cause hint above.\n"
                    f"  2. Check Airflow UI: {AIRFLOW_URL}/dags/{run['dag_id']}/grid\n"
                    "  3. If transient (timeout/connectivity): clear the failed task and retry.\n"
                    "  4. If data issue: verify source file availability and schema.\n"
                    "  5. This incident is eligible for AI auto-remediation.\n"
                    "\n"
                    "AI AUTO-REMEDIATION TOOLS\n"
                    "  - airflow_retrigger_failed_dag: retry from failure point\n"
                    "  - airflow_clear_task_instance:  clear and re-queue a task\n"
                    "  - airflow_get_dag_failure_summary: get full failure context\n"
                    "\n"
                    "GENERATED BY: Unified Infrastructure Monitor (automated)\n"
                    "===================================================="
                ),
                'priority': priority,
                'urgency': priority,
                'impact': priority,
                'category': 'Data Pipeline',
                'subcategory': 'Airflow',
                'assignment_group': 'Data Engineering',
            })

            # Publish to Kafka (airflow.failures)
            event = {
                'event_type': 'airflow_dag_failure',
                'timestamp': datetime.now().isoformat(),
                'source': 'unified_infra_monitor',
                'dag_failure': {
                    'dag_id': run['dag_id'],
                    'dag_run_id': run['dag_run_id'],
                    'execution_date': run['execution_date'],
                    'start_date': run['start_date'],
                    'end_date': run['end_date'],
                    'state': run['state'],
                    'failed_tasks': failed_tasks,
                    'airflow_url': AIRFLOW_URL,
                },
                'incident': incident_result,
                'remediation_hints': {
                    'can_retry': True,
                    'retry_endpoint': f"{AIRFLOW_URL}/api/v1/dags/{run['dag_id']}/dagRuns",
                    'clear_task_endpoint': f"{AIRFLOW_URL}/api/v1/dags/{run['dag_id']}/clearTaskInstances",
                    'mcp_tools': {
                        'retrigger': 'airflow_retrigger_failed_dag',
                        'clear_task': 'airflow_clear_task_instance',
                        'failure_summary': 'airflow_get_dag_failure_summary',
                        'trigger_dag': 'airflow_trigger_dag',
                        'get_status': 'airflow_get_dag_status',
                    },
                    'recommended_action': 'Use airflow_retrigger_failed_dag to auto-recover from failure point',
                },
            }
            self.publish_to_kafka('airflow.failures', run['dag_id'], event)

            # Also publish to incident.created for the LangGraph workflow
            incident_event = {
                'event_type': 'incident.created',
                'timestamp': datetime.now().isoformat(),
                'source': 'unified_infra_monitor',
                'incident': {
                    'number': incident_result.get('incident_id', 'UNKNOWN'),
                    'sys_id': incident_result.get('sys_id', ''),
                    'short_description': f"DAG '{run['dag_id']}' failed - {total_tasks} task(s)",
                    'description': f"Root cause hint: {error_hint}",
                    'category': 'Data Pipeline',
                    'subcategory': 'Airflow',
                    'priority': priority,
                    'state': 'new',
                },
                'context': {
                    'dag_id': run['dag_id'],
                    'dag_run_id': run['dag_run_id'],
                    'failed_tasks': [t['task_id'] for t in failed_tasks],
                    'remediation_available': True,
                },
            }
            self.publish_to_kafka('incident.created', incident_result.get('incident_id', run['dag_id']), incident_event)

            self.processed_dag_runs[run_key] = datetime.now()

        print(f"   [{self.name}] Processed {len(failed_runs)} failed DAG run(s)")

    def list_status(self):
        """List recent Airflow DAG runs."""
        print(f"\n[{self.name}] Recent DAG runs (last hour):")
        try:
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(hours=1)
            response = requests.get(
                f"{AIRFLOW_URL}/api/v1/dags/~/dagRuns",
                auth=self.airflow_auth,
                params={'start_date_gte': start_time.isoformat() + 'Z', 'limit': 50},
                headers={'Content-Type': 'application/json'},
                timeout=30,
            )
            if response.status_code == 200:
                runs = response.json().get('dag_runs', [])
                by_state = {}
                for r in runs:
                    by_state.setdefault(r['state'], []).append(r)
                for state, state_runs in sorted(by_state.items()):
                    icon = {'success': '+', 'failed': '!', 'running': '~', 'queued': '?'}.get(state, ' ')
                    print(f"   [{icon}] {state}: {len(state_runs)}")
                    for r in state_runs[:3]:
                        print(f"       - {r['dag_id']} ({r['dag_run_id']})")
                print(f"   Total: {len(runs)} runs")
            else:
                print(f"   Error: {response.status_code}")
        except requests.exceptions.ConnectionError:
            print(f"   Cannot connect to Airflow at {AIRFLOW_URL}")
        except Exception as e:
            print(f"   Error: {e}")


# =============================================================================
# GCP VM Monitor
# =============================================================================

class GCPVMMonitor(BaseMonitor):
    """Monitors GCP Compute Engine VMs for stopped/terminated instances."""

    def __init__(self, kafka_producer, snow_auth):
        super().__init__(
            name="GCP-VM",
            kafka_producer=kafka_producer,
            snow_auth=snow_auth,
            poll_interval=GCP_POLL_INTERVAL,
        )
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = GOOGLE_APPLICATION_CREDENTIALS
        try:
            from google.cloud import compute_v1
            self.compute_client = compute_v1.InstancesClient()
            self._compute_v1 = compute_v1
            self._gcp_available = True
        except Exception as e:
            print(f"   [{self.name}] GCP Compute not available: {e}")
            self.compute_client = None
            self._gcp_available = False

        self.processed_vms: Dict[str, datetime] = {}
        self.existing_incidents: Dict[str, str] = {}  # vm_name -> incident_number
        self._load_existing_incidents()

    def _load_existing_incidents(self):
        """Load open GCP incidents from ServiceNow to avoid duplicates."""
        results = self.query_servicenow(
            'short_descriptionLIKE[GCP Alert]^stateIN1,2,3',
            'number,short_description,sys_id,state',
            limit=100,
        )
        for inc in results:
            desc = inc.get('short_description', '')
            if 'VM instance' in desc:
                try:
                    vm_name = desc.split('VM instance ')[1].split(' is ')[0]
                    self.existing_incidents[vm_name] = inc.get('number')
                except IndexError:
                    pass
        if self.existing_incidents:
            print(f"   [{self.name}] Loaded {len(self.existing_incidents)} existing open incidents")

    def _has_open_incident(self, vm_name: str) -> bool:
        """Check if VM already has an open incident (cache + live query)."""
        if vm_name in self.existing_incidents:
            return True
        # Live check
        results = self.query_servicenow(
            f'short_descriptionLIKEVM instance {vm_name}^stateIN1,2,3',
            'number,sys_id',
            limit=1,
        )
        if results:
            self.existing_incidents[vm_name] = results[0].get('number', '')
            return True
        return False

    def _get_all_vms(self) -> List[Dict]:
        """Get all VMs across configured zones."""
        if not self._gcp_available:
            return []
        all_vms = []
        for zone in GCP_ZONES:
            try:
                request = self._compute_v1.ListInstancesRequest(
                    project=GCP_PROJECT_ID, zone=zone,
                )
                instances = list(self.compute_client.list(request=request))
                for inst in instances:
                    all_vms.append({
                        'name': inst.name,
                        'zone': zone,
                        'status': inst.status,
                        'machine_type': inst.machine_type.split('/')[-1] if inst.machine_type else 'unknown',
                        'creation_timestamp': inst.creation_timestamp,
                        'id': inst.id,
                    })
            except Exception as e:
                print(f"   [{self.name}] Error fetching VMs from {zone}: {e}")
        return all_vms

    def check_and_process(self):
        print(f"[{self.name}] Scanning at {datetime.now().strftime('%H:%M:%S')}...")

        if not self._gcp_available:
            print(f"   [{self.name}] GCP Compute not available, skipping")
            return

        # Refresh dedup cache
        self._load_existing_incidents()

        vms = self._get_all_vms()
        stopped_vms = []

        for vm in vms:
            vm_key = f"{vm['name']}-{vm['zone']}"
            if vm['status'] in ['TERMINATED', 'STOPPED', 'SUSPENDED']:
                if vm_key in self.processed_vms:
                    if (datetime.now() - self.processed_vms[vm_key]).seconds < 1800:
                        continue
                stopped_vms.append(vm)
                print(f"   [{self.name}] Stopped VM: {vm['name']} ({vm['status']}) in {vm['zone']}")
            else:
                # Running — clear caches so new incidents can be created if it stops again
                self.processed_vms.pop(vm_key, None)
                self.existing_incidents.pop(vm['name'], None)

        if not stopped_vms:
            print(f"   [{self.name}] All VMs are running")
            return

        created = 0
        skipped = 0
        for vm in stopped_vms:
            vm_key = f"{vm['name']}-{vm['zone']}"

            if self._has_open_incident(vm['name']):
                print(f"   [{self.name}] Skipping {vm['name']} - open incident exists: {self.existing_incidents[vm['name']]}")
                skipped += 1
                self.publish_to_kafka('gcp.alerts', vm['name'], {
                    'event_type': 'gcp_vm_alert',
                    'timestamp': datetime.now().isoformat(),
                    'source': 'unified_infra_monitor',
                    'alert': {'type': 'VM_STOPPED', 'vm_name': vm['name'], 'zone': vm['zone'],
                              'project': GCP_PROJECT_ID, 'status': vm['status']},
                    'incident': {'incident_id': self.existing_incidents[vm['name']], 'already_exists': True},
                })
                continue

            # Determine priority based on VM status
            gcp_priority = '1' if vm['status'] == 'TERMINATED' else '2'
            status_detail = {
                'TERMINATED': 'VM was terminated (possibly preempted or shutdown by GCP). Requires manual restart.',
                'STOPPED': 'VM was stopped (possibly by user, scheduler, or billing). Can be started via console or API.',
                'SUSPENDED': 'VM is suspended (memory preserved). Can be resumed with minimal downtime.',
            }.get(vm['status'], f"VM is in unexpected state: {vm['status']}")

            incident_result = self.create_servicenow_incident({
                'short_description': (
                    f"[GCP] VM '{vm['name']}' is {vm['status'].lower()} "
                    f"in {vm['zone']} "
                    f"- {datetime.now().strftime('%Y-%m-%d %H:%M')}"
                ),
                'description': (
                    "====================================================\n"
                    "GCP VM OUTAGE - AUTO-DETECTED\n"
                    "====================================================\n"
                    "\n"
                    "INSTANCE DETAILS\n"
                    f"  Name:         {vm['name']}\n"
                    f"  Zone:         {vm['zone']}\n"
                    f"  Project:      {GCP_PROJECT_ID}\n"
                    f"  Machine Type: {vm['machine_type']}\n"
                    f"  Status:       {vm['status']}\n"
                    f"  Created:      {vm.get('creation_timestamp', 'N/A')}\n"
                    "\n"
                    "STATUS EXPLANATION\n"
                    f"  {status_detail}\n"
                    "\n"
                    "IMPACT\n"
                    "  Any applications, services, or scheduled jobs running on this\n"
                    "  VM are currently unavailable. Downstream systems depending on\n"
                    "  this instance may also be affected.\n"
                    "\n"
                    "RECOMMENDED REMEDIATION\n"
                    "  1. Check serial console logs for crash/shutdown reason:\n"
                    f"     Console > Compute Engine > {vm['name']} > Serial port 1\n"
                    "  2. Review Cloud Logging for errors around the stop time:\n"
                    f"     resource.type=\"gce_instance\" resource.labels.instance_id=\"{vm.get('id', 'N/A')}\"\n"
                    "  3. If safe to restart, start the VM:\n"
                    f"     gcloud compute instances start {vm['name']} --zone={vm['zone']} --project={GCP_PROJECT_ID}\n"
                    "  4. After restart, verify application health and connectivity.\n"
                    "  5. If preempted, consider migrating to a non-preemptible machine type.\n"
                    "\n"
                    "QUICK LINKS\n"
                    f"  VM Detail:  https://console.cloud.google.com/compute/instancesDetail/zones/{vm['zone']}/instances/{vm['name']}?project={GCP_PROJECT_ID}\n"
                    f"  Logs:       https://console.cloud.google.com/logs/query;query=resource.type%3D%22gce_instance%22%20resource.labels.instance_id%3D%22{vm.get('id', '')}%22?project={GCP_PROJECT_ID}\n"
                    f"  All VMs:    https://console.cloud.google.com/compute/instances?project={GCP_PROJECT_ID}\n"
                    "\n"
                    "GENERATED BY: Unified Infrastructure Monitor (automated)\n"
                    "===================================================="
                ),
                'priority': gcp_priority,
                'urgency': gcp_priority,
                'impact': gcp_priority,
                'category': 'Infrastructure',
                'subcategory': 'Cloud',
                'assignment_group': 'Cloud Operations',
            })

            if incident_result.get('success'):
                self.existing_incidents[vm['name']] = incident_result.get('incident_id')
                created += 1

            self.publish_to_kafka('gcp.alerts', vm['name'], {
                'event_type': 'gcp_vm_alert',
                'timestamp': datetime.now().isoformat(),
                'source': 'unified_infra_monitor',
                'alert': {'type': 'VM_STOPPED', 'vm_name': vm['name'], 'zone': vm['zone'],
                          'project': GCP_PROJECT_ID, 'status': vm['status'],
                          'machine_type': vm['machine_type']},
                'incident': incident_result,
            })
            self.processed_vms[vm_key] = datetime.now()

        print(f"   [{self.name}] Created: {created}, Skipped: {skipped}")

    def list_status(self):
        """List all GCP VMs and their status."""
        print(f"\n[{self.name}] VM Status (project: {GCP_PROJECT_ID}):")
        if not self._gcp_available:
            print(f"   GCP Compute not available")
            return
        vms = self._get_all_vms()
        running = [v for v in vms if v['status'] == 'RUNNING']
        stopped = [v for v in vms if v['status'] != 'RUNNING']
        print(f"   [+] Running: {len(running)}")
        for vm in running:
            print(f"       - {vm['name']} ({vm['zone']})")
        print(f"   [!] Stopped/Other: {len(stopped)}")
        for vm in stopped:
            print(f"       - {vm['name']} ({vm['zone']}) - {vm['status']}")
        print(f"   Total: {len(vms)} VMs")


# =============================================================================
# Unified Orchestrator
# =============================================================================

class InfrastructureMonitor:
    """Runs multiple infrastructure monitors concurrently."""

    def __init__(self, sources: Optional[List[str]] = None):
        print("=" * 60)
        print("  Unified Infrastructure Monitor")
        print("=" * 60)

        # Shared resources
        self.kafka_producer = self._init_kafka()
        self.snow_auth = HTTPBasicAuth(SNOW_USER, SNOW_PASS)

        # Initialize requested monitors
        enabled = sources or ['airflow', 'gcp']
        self.monitors: List[BaseMonitor] = []

        if 'airflow' in enabled:
            self.monitors.append(AirflowMonitor(self.kafka_producer, self.snow_auth))
            print(f"  [+] Airflow monitor enabled (interval: {AIRFLOW_POLL_INTERVAL}s)")

        if 'gcp' in enabled:
            self.monitors.append(GCPVMMonitor(self.kafka_producer, self.snow_auth))
            print(f"  [+] GCP VM monitor enabled (interval: {GCP_POLL_INTERVAL}s)")

        print(f"  ServiceNow: {SNOW_URL}")
        print(f"  Kafka: {KAFKA_BOOTSTRAP}")
        print("=" * 60 + "\n")

    def _init_kafka(self) -> Optional[KafkaProducer]:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',
                retries=3,
            )
            print("  [+] Kafka producer initialized")
            return producer
        except Exception as e:
            print(f"  [!] Kafka not available: {e}")
            return None

    def run(self):
        """Run all monitors concurrently in threads."""
        threads: List[threading.Thread] = []
        for monitor in self.monitors:
            t = threading.Thread(target=monitor.run_loop, name=f"monitor-{monitor.name}", daemon=True)
            threads.append(t)
            t.start()

        # Wait for SIGINT / SIGTERM
        def _shutdown(signum, frame):
            print("\nShutting down all monitors...")
            for m in self.monitors:
                m.stop()

        signal.signal(signal.SIGINT, _shutdown)
        signal.signal(signal.SIGTERM, _shutdown)

        # Block until all threads finish
        for t in threads:
            t.join()

        if self.kafka_producer:
            self.kafka_producer.close()
        print("All monitors stopped.")

    def check_once(self):
        """Run all monitors once and exit."""
        print("Running single check across all sources...\n")
        for monitor in self.monitors:
            monitor.check_and_process()
        if self.kafka_producer:
            self.kafka_producer.close()
        print("\nDone.")

    def list_status(self):
        """List status from all monitors."""
        print("Infrastructure Status Report")
        print("=" * 60)
        for monitor in self.monitors:
            monitor.list_status()
        print()


# =============================================================================
# CLI
# =============================================================================

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(
        description='Unified Infrastructure Monitor - Airflow DAGs + GCP VMs',
    )
    parser.add_argument('--once', action='store_true', help='Run single check and exit')
    parser.add_argument('--list', action='store_true', help='List current status and exit')
    parser.add_argument('--interval', type=int, help='Override poll interval for all sources')
    parser.add_argument(
        '--source', choices=['airflow', 'gcp', 'all'], default='all',
        help='Which source(s) to monitor (default: all)',
    )
    args = parser.parse_args()

    if args.interval:
        AIRFLOW_POLL_INTERVAL = args.interval
        GCP_POLL_INTERVAL = args.interval

    sources = ['airflow', 'gcp'] if args.source == 'all' else [args.source]
    monitor = InfrastructureMonitor(sources=sources)

    if args.list:
        monitor.list_status()
    elif args.once:
        monitor.check_once()
    else:
        monitor.run()
