#!/usr/bin/env python3
"""
Push sample Jira tickets for the Data Pipeline Agent demo.
"""

import os
import requests
from requests.auth import HTTPBasicAuth
import json

# Jira configuration from environment
JIRA_URL = os.getenv("JIRA_URL", "https://samrattidke.atlassian.net")
JIRA_USERNAME = os.getenv("JIRA_USERNAME", "samrat.tidke@gmail.com")
JIRA_API_TOKEN = os.getenv("JIRA_API_TOKEN")
JIRA_PROJECT_KEY = os.getenv("JIRA_PROJECT_KEY", "SCRUM")

# Jira API endpoints
API_BASE = f"{JIRA_URL}/rest/api/3"

# Auth
auth = HTTPBasicAuth(JIRA_USERNAME, JIRA_API_TOKEN)
headers = {
    "Accept": "application/json",
    "Content-Type": "application/json"
}

# Sample tickets to create
SAMPLE_TICKETS = [
    {
        "summary": "Build Cobrix EBCDIC File Parser Pipeline for Legacy Mainframe Data",
        "description": """h2. Objective
Ingest legacy mainframe COBOL/EBCDIC files into the data lake using Cobrix parser.

h2. Source Details
* *File Format*: EBCDIC encoded fixed-width files
* *Copybook*: CUSTOMER-MASTER.cpy
* *Location*: gs://legacy-mainframe-exports/customer/*.dat
* *Frequency*: Daily batch at 2 AM EST
* *Volume*: ~5 million records/day

h2. Schema (from COBOL Copybook)
{code:cobol}
01 CUSTOMER-RECORD.
   05 CUST-ID          PIC 9(10).
   05 CUST-NAME        PIC X(50).
   05 CUST-ADDRESS     PIC X(100).
   05 CUST-CITY        PIC X(30).
   05 CUST-STATE       PIC X(2).
   05 CUST-ZIP         PIC 9(5).
   05 CUST-PHONE       PIC 9(10).
   05 CUST-BALANCE     PIC S9(9)V99 COMP-3.
   05 CUST-STATUS      PIC X(1).
   05 CUST-OPEN-DATE   PIC 9(8).
{code}

h2. Data Type Mappings
||COBOL Field||PIC Clause||Spark Type||Notes||
|CUST-ID|PIC 9(10)|LongType|Numeric ID|
|CUST-NAME|PIC X(50)|StringType|Fixed-width text|
|CUST-BALANCE|PIC S9(9)V99 COMP-3|DecimalType(11,2)|Packed decimal (BCD)|
|CUST-OPEN-DATE|PIC 9(8)|DateType|YYYYMMDD format|

h2. Requirements
* Parse EBCDIC to UTF-8 using Cobrix library
* Handle COMP-3 (packed decimal) fields for balance
* Validate PII fields (phone, address) and apply masking
* Load to BigQuery bronze zone with schema enforcement
* Implement SCD Type 2 for customer dimension in silver zone

h2. Technical Notes
* Use {{spark-cobol}} library (com.github.AbsaOSS:cobrix)
* Copybook path: gs://copybooks/CUSTOMER-MASTER.cpy
* Character set: EBCDIC (cp037)
* Record format: Fixed-block (FB)

h2. Acceptance Criteria
* Pipeline processes all EBCDIC files from source
* COMP-3 fields correctly converted to decimal
* PII masking applied to phone/address fields
* Data quality checks pass (>99.5% completeness)
* Audit logging enabled for compliance
* SCD Type 2 history tracking in silver zone""",
        "issue_type": "Story",
        "priority": "High",
        "labels": ["data-pipeline", "ebcdic", "mainframe", "cobrix", "legacy-migration", "pii"]
    },
    {
        "summary": "Create Sales Transaction Pipeline from Oracle CDC",
        "description": """h2. Objective
Implement CDC-based pipeline to capture sales transactions from Oracle ERP.

h2. Source
* Oracle 19c
* Schema: SALES.TRANSACTIONS
* CDC via LogMiner

h2. Target
* BigQuery: sales_domain.transactions_silver
* Real-time streaming with 5-minute micro-batches

h2. Requirements
* Capture INSERT, UPDATE, DELETE operations
* Maintain transaction ordering
* Handle schema evolution
* Support exactly-once delivery semantics
* Implement late-arriving data handling

h2. Technical Approach
* Use Debezium connector for Oracle CDC
* Kafka as intermediate message queue
* Spark Structured Streaming for processing
* BigQuery streaming inserts for low latency""",
        "issue_type": "Story",
        "priority": "Highest",
        "labels": ["data-pipeline", "oracle", "cdc", "streaming", "real-time"]
    },
    {
        "summary": "Migrate SSIS Package: Daily Inventory ETL",
        "description": """h2. Current State
* DTSX Package: InventoryDaily.dtsx
* Runs on SQL Server Integration Services
* Processes 2M records daily
* Contains 15 data flow tasks
* Uses OLEDB connections to SQL Server

h2. Target State
* Airflow DAG with PySpark jobs
* Same business logic preserved
* Improved monitoring and alerting
* Cloud-native architecture

h2. Migration Steps
# Parse DTSX package XML
# Extract connection managers
# Map OLEDB sources to BigQuery/GCS
# Convert T-SQL transforms to PySpark
# Generate Airflow DAG
# Validate output parity

h2. Acceptance Criteria
* All 15 data flows migrated
* Output matches within 0.01% tolerance
* Processing time <= current runtime
* Alerting configured for failures""",
        "issue_type": "Task",
        "priority": "Medium",
        "labels": ["data-pipeline", "dtsx-migration", "ssis", "inventory", "legacy"]
    },
    {
        "summary": "Build Customer 360 Gold Layer Aggregation",
        "description": """h2. Objective
Create Gold zone aggregation for Customer 360 view combining multiple silver layer sources.

h2. Source Tables (Silver)
* customers_silver - Customer master data
* orders_silver - Order transactions
* support_tickets_silver - Support interactions
* marketing_interactions_silver - Campaign responses

h2. Target
* customer_360_gold (Star Schema)
* Daily refresh with incremental updates

h2. Schema Design
||Column||Type||Source||
|customer_id|STRING|customers_silver|
|customer_name|STRING|customers_silver|
|lifetime_value|DECIMAL|orders_silver (SUM)|
|total_orders|INT|orders_silver (COUNT)|
|avg_order_value|DECIMAL|orders_silver (AVG)|
|last_order_date|DATE|orders_silver (MAX)|
|support_tickets_count|INT|support_tickets_silver|
|marketing_engagement_score|FLOAT|marketing_interactions_silver|

h2. Business Rules
* LTV calculation: SUM(order_total) over all time
* Engagement score: Weighted sum of opens, clicks, conversions
* Only include active customers (status = 'ACTIVE')""",
        "issue_type": "Story",
        "priority": "High",
        "labels": ["data-pipeline", "gold-zone", "customer-360", "aggregation", "star-schema"]
    },
    {
        "summary": "Fix Data Quality Issues in Product Catalog Pipeline",
        "description": """h2. Issue
Data quality checks failing in product catalog pipeline:
* NULL values in required fields (product_name, category_id)
* Duplicate product_ids detected
* Invalid price values (negative amounts)

h2. Root Cause
Source system sending malformed records after recent upgrade.

h2. Impact
* Gold layer product dimension has stale data
* Downstream reports showing incorrect metrics
* Customer-facing catalog has missing products

h2. Investigation Findings
{code}
-- Null check results
SELECT COUNT(*) FROM products_bronze WHERE product_name IS NULL
-- Result: 1,247 records

-- Duplicate check
SELECT product_id, COUNT(*)
FROM products_bronze
GROUP BY product_id
HAVING COUNT(*) > 1
-- Result: 89 duplicate IDs

-- Invalid prices
SELECT COUNT(*) FROM products_bronze WHERE price < 0
-- Result: 156 records
{code}

h2. Proposed Fix
# Add source-side validation before ingestion
# Implement quarantine table for bad records
# Add alerting for DQ threshold breaches
# Backfill corrected data from source""",
        "issue_type": "Bug",
        "priority": "Highest",
        "labels": ["bug", "data-quality", "product-catalog", "urgent", "dq-failure"]
    },
    {
        "summary": "Ingest Kafka Clickstream Events to Bronze Zone",
        "description": """h2. Objective
Set up streaming pipeline for website clickstream events.

h2. Source
* Kafka Topic: web.clickstream.events
* Format: Avro with Schema Registry
* Volume: ~100K events/minute peak
* Retention: 7 days

h2. Event Schema
{code:json}
{
  "event_id": "uuid",
  "event_type": "page_view|click|scroll|form_submit",
  "user_id": "string",
  "session_id": "string",
  "page_url": "string",
  "referrer_url": "string",
  "timestamp": "long",
  "properties": {
    "element_id": "string",
    "element_class": "string",
    "scroll_depth": "int"
  },
  "user_agent": "string",
  "ip_address": "string"
}
{code}

h2. Target
* BigQuery: clickstream_domain.events_bronze
* Partitioned by event_date
* Clustered by user_id, session_id

h2. Requirements
* Max latency: 5 minutes end-to-end
* Handle schema evolution gracefully
* PII masking for ip_address field
* Deduplication on event_id
* Dead letter queue for malformed events""",
        "issue_type": "Story",
        "priority": "Medium",
        "labels": ["data-pipeline", "kafka", "streaming", "clickstream", "avro"]
    }
]

def get_issue_type_id(issue_type_name: str) -> str:
    """Get issue type ID from name."""
    response = requests.get(
        f"{API_BASE}/issuetype",
        headers=headers,
        auth=auth
    )
    if response.status_code == 200:
        for it in response.json():
            if it["name"].lower() == issue_type_name.lower():
                return it["id"]
    # Default to Story if not found
    return "10001"

def get_priority_id(priority_name: str) -> str:
    """Get priority ID from name."""
    response = requests.get(
        f"{API_BASE}/priority",
        headers=headers,
        auth=auth
    )
    if response.status_code == 200:
        for p in response.json():
            if p["name"].lower() == priority_name.lower():
                return p["id"]
    # Default to Medium
    return "3"

def create_issue(ticket: dict) -> dict:
    """Create a Jira issue."""
    # Convert description to Atlassian Document Format (ADF)
    description_adf = {
        "type": "doc",
        "version": 1,
        "content": [
            {
                "type": "paragraph",
                "content": [
                    {
                        "type": "text",
                        "text": ticket["description"]
                    }
                ]
            }
        ]
    }

    payload = {
        "fields": {
            "project": {
                "key": JIRA_PROJECT_KEY
            },
            "summary": ticket["summary"],
            "description": description_adf,
            "issuetype": {
                "name": ticket["issue_type"]
            },
            "priority": {
                "name": ticket["priority"]
            },
            "labels": ticket["labels"]
        }
    }

    response = requests.post(
        f"{API_BASE}/issue",
        headers=headers,
        auth=auth,
        data=json.dumps(payload)
    )

    return response

def main():
    print("=" * 60)
    print("Pushing Sample Jira Tickets")
    print("=" * 60)
    print(f"Jira URL: {JIRA_URL}")
    print(f"Project: {JIRA_PROJECT_KEY}")
    print(f"Tickets to create: {len(SAMPLE_TICKETS)}")
    print("=" * 60)

    if not JIRA_API_TOKEN:
        print("ERROR: JIRA_API_TOKEN not set!")
        return

    created = []
    failed = []

    for i, ticket in enumerate(SAMPLE_TICKETS, 1):
        print(f"\n[{i}/{len(SAMPLE_TICKETS)}] Creating: {ticket['summary'][:50]}...")

        response = create_issue(ticket)

        if response.status_code in [200, 201]:
            data = response.json()
            issue_key = data.get("key", "Unknown")
            created.append({
                "key": issue_key,
                "summary": ticket["summary"]
            })
            print(f"    SUCCESS: {issue_key}")
            print(f"    URL: {JIRA_URL}/browse/{issue_key}")
        else:
            failed.append({
                "summary": ticket["summary"],
                "error": response.text
            })
            print(f"    FAILED: {response.status_code}")
            print(f"    Error: {response.text[:200]}")

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Created: {len(created)}")
    print(f"Failed: {len(failed)}")

    if created:
        print("\nCreated Issues:")
        for item in created:
            print(f"  - {item['key']}: {item['summary'][:50]}")

    if failed:
        print("\nFailed Issues:")
        for item in failed:
            print(f"  - {item['summary'][:50]}")
            print(f"    Error: {item['error'][:100]}")

if __name__ == "__main__":
    main()
