locals {
  topics = [
    # Incident workflow (12 topics)
    "incident-created",
    "incident-enriched",
    "incident-plan-generated",
    "incident-requires-approval",
    "incident-approved",
    "incident-rejected",
    "incident-executed",
    "incident-verified",
    "incident-close-requested",
    "incident-close-execute",
    "incident-closed",
    "incident-postmortem-ready",
    # Pipeline workflow (12 topics)
    "pipeline-requested",
    "pipeline-planned",
    "pipeline-generated",
    "pipeline-validated",
    "pipeline-requires-approval",
    "pipeline-approved",
    "pipeline-rejected",
    "pipeline-deploy-execute",
    "pipeline-deployed",
    "pipeline-completed",
    "pipeline-failed",
    "pipeline-mr-created",
    # MCP command channels (7 topics)
    "mcp-servicenow-commands",
    "mcp-jira-commands",
    "mcp-github-commands",
    "mcp-airflow-commands",
    "mcp-rag-commands",
    "mcp-gcs-commands",
    "mcp-llm-commands",
    # Airflow integration (4 topics)
    "airflow-trigger-dag",
    "airflow-dag-completed",
    "airflow-dag-failed",
    "airflow-dag-status",
    # External ingestion (4 topics)
    "external-servicenow-incidents",
    "external-jira-tickets",
    "external-gcp-alerts",
    "external-github-events",
    # Agent coordination (4 topics)
    "agent-events",
    "agent-heartbeat",
    "swarm-task-queue",
    "swarm-results",
    # Monitoring (4 topics)
    "monitoring-alerts",
    "monitoring-metrics",
    "proactive-alerts",
    "dead-letter",
  ]

  # Subscriptions: topic → subscriber service account
  subscriptions = {
    "incident-created"            = { sa = var.worker_sa, service = "event-orchestrator" }
    "incident-enriched"           = { sa = var.worker_sa, service = "incident-consumer" }
    "incident-plan-generated"     = { sa = var.worker_sa, service = "incident-consumer" }
    "incident-requires-approval"  = { sa = var.backend_sa, service = "backend-api" }
    "incident-approved"           = { sa = var.worker_sa, service = "incident-consumer" }
    "incident-rejected"           = { sa = var.worker_sa, service = "incident-consumer" }
    "incident-executed"           = { sa = var.worker_sa, service = "incident-consumer" }
    "incident-closed"             = { sa = var.worker_sa, service = "post-mortem-agent" }
    "pipeline-requested"          = { sa = var.worker_sa, service = "pipeline-consumer" }
    "pipeline-validated"          = { sa = var.worker_sa, service = "pipeline-consumer" }
    "pipeline-requires-approval"  = { sa = var.backend_sa, service = "backend-api" }
    "pipeline-approved"           = { sa = var.worker_sa, service = "pipeline-consumer" }
    "pipeline-failed"             = { sa = var.worker_sa, service = "pipeline-consumer" }
    "external-servicenow-incidents" = { sa = var.worker_sa, service = "event-orchestrator" }
    "external-jira-tickets"       = { sa = var.worker_sa, service = "jira-consumer" }
    "external-gcp-alerts"         = { sa = var.worker_sa, service = "proactive-monitor" }
    "proactive-alerts"            = { sa = var.worker_sa, service = "incident-consumer" }
    "agent-events"                = { sa = var.worker_sa, service = "event-orchestrator" }
  }
}

resource "google_pubsub_topic" "topics" {
  for_each = toset(local.topics)
  project  = var.project_id
  name     = each.key

  message_retention_duration = "604800s" # 7 days
}

resource "google_pubsub_subscription" "subscriptions" {
  for_each = local.subscriptions
  project  = var.project_id
  name     = "${each.key}-${each.value.service}-sub"
  topic    = google_pubsub_topic.topics[each.key].name

  ack_deadline_seconds       = 60
  message_retention_duration = "86400s" # 1 day

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.topics["dead-letter"].name
    max_delivery_attempts = 5
  }

  expiration_policy {
    ttl = "" # never expire
  }
}

# IAM: backend SA can publish + subscribe
resource "google_pubsub_topic_iam_member" "backend_publisher" {
  for_each = toset(local.topics)
  project  = var.project_id
  topic    = google_pubsub_topic.topics[each.key].name
  role     = "roles/pubsub.publisher"
  member   = "serviceAccount:${var.backend_sa}"
}

resource "google_pubsub_subscription_iam_member" "backend_subscriber" {
  for_each     = local.subscriptions
  project      = var.project_id
  subscription = google_pubsub_subscription.subscriptions[each.key].name
  role         = "roles/pubsub.subscriber"
  member       = "serviceAccount:${var.backend_sa}"
}

# IAM: worker SA can publish + subscribe
resource "google_pubsub_topic_iam_member" "worker_publisher" {
  for_each = toset(local.topics)
  project  = var.project_id
  topic    = google_pubsub_topic.topics[each.key].name
  role     = "roles/pubsub.publisher"
  member   = "serviceAccount:${var.worker_sa}"
}

resource "google_pubsub_subscription_iam_member" "worker_subscriber" {
  for_each     = local.subscriptions
  project      = var.project_id
  subscription = google_pubsub_subscription.subscriptions[each.key].name
  role         = "roles/pubsub.subscriber"
  member       = "serviceAccount:${var.worker_sa}"
}
