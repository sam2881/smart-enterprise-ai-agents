resource "google_monitoring_notification_channel" "email" {
  display_name = "AI Agent Platform Alerts"
  type         = "email"
  project      = var.project_id

  labels = {
    email_address = var.alert_email
  }
}

# Cloud Run error rate alert
resource "google_monitoring_alert_policy" "cloud_run_errors" {
  display_name = "[${var.env}] Cloud Run High Error Rate"
  project      = var.project_id
  combiner     = "OR"

  conditions {
    display_name = "Cloud Run 5xx error rate > 5%"
    condition_threshold {
      filter          = "resource.type=\"cloud_run_revision\" AND metric.type=\"run.googleapis.com/request_count\" AND metric.labels.response_code_class=\"5xx\""
      duration        = "300s"
      comparison      = "COMPARISON_GT"
      threshold_value = 0.05
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_RATE"
      }
    }
  }

  notification_channels = [google_monitoring_notification_channel.email.name]

  alert_strategy {
    auto_close = "1800s"
  }
}

# Pub/Sub dead-letter backlog alert
resource "google_monitoring_alert_policy" "pubsub_dead_letter" {
  display_name = "[${var.env}] Pub/Sub Dead Letter Backlog"
  project      = var.project_id
  combiner     = "OR"

  conditions {
    display_name = "Dead letter topic has > 10 undelivered messages"
    condition_threshold {
      filter          = "resource.type=\"pubsub_subscription\" AND resource.label.subscription_id=has_substring(\"dead-letter\") AND metric.type=\"pubsub.googleapis.com/subscription/num_undelivered_messages\""
      duration        = "300s"
      comparison      = "COMPARISON_GT"
      threshold_value = 10
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_MEAN"
      }
    }
  }

  notification_channels = [google_monitoring_notification_channel.email.name]
}

# Cloud SQL CPU alert
resource "google_monitoring_alert_policy" "sql_cpu" {
  display_name = "[${var.env}] Cloud SQL High CPU"
  project      = var.project_id
  combiner     = "OR"

  conditions {
    display_name = "Cloud SQL CPU > 80%"
    condition_threshold {
      filter          = "resource.type=\"cloudsql_database\" AND metric.type=\"cloudsql.googleapis.com/database/cpu/utilization\""
      duration        = "300s"
      comparison      = "COMPARISON_GT"
      threshold_value = 0.8
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_MEAN"
      }
    }
  }

  notification_channels = [google_monitoring_notification_channel.email.name]
}

# GKE memory pressure alert
resource "google_monitoring_alert_policy" "gke_memory" {
  display_name = "[${var.env}] GKE Memory Pressure"
  project      = var.project_id
  combiner     = "OR"

  conditions {
    display_name = "GKE pod memory > 90%"
    condition_threshold {
      filter          = "resource.type=\"k8s_container\" AND metric.type=\"kubernetes.io/container/memory/used_bytes\""
      duration        = "300s"
      comparison      = "COMPARISON_GT"
      threshold_value = 17179869184 # 16Gi in bytes
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_MEAN"
      }
    }
  }

  notification_channels = [google_monitoring_notification_channel.email.name]
}

# Uptime check for backend API
resource "google_monitoring_uptime_check_config" "backend_health" {
  display_name = "[${var.env}] Backend API Health"
  project      = var.project_id
  timeout      = "10s"
  period       = "60s"

  http_check {
    path         = "/health"
    port         = 443
    use_ssl      = true
    validate_ssl = true
  }

  monitored_resource {
    type = "uptime_url"
    labels = {
      project_id = var.project_id
      host       = var.backend_url
    }
  }
}

# Logging-based metric for LangGraph workflow failures
resource "google_logging_metric" "workflow_failures" {
  name    = "langgraph_workflow_failures_${var.env}"
  project = var.project_id
  filter  = "resource.type=\"cloud_run_revision\" AND textPayload=~\"error_agent\" AND severity>=ERROR"

  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "INT64"
    unit        = "1"
    labels {
      key         = "agent"
      value_type  = "STRING"
      description = "LangGraph node that failed"
    }
  }

  label_extractors = {
    "agent" = "EXTRACT(jsonPayload.error_agent)"
  }
}

resource "google_monitoring_alert_policy" "workflow_failures" {
  display_name = "[${var.env}] LangGraph Workflow Failures"
  project      = var.project_id
  combiner     = "OR"

  conditions {
    display_name = "Workflow failures > 5 in 5 minutes"
    condition_threshold {
      filter          = "metric.type=\"logging.googleapis.com/user/${google_logging_metric.workflow_failures.name}\""
      duration        = "300s"
      comparison      = "COMPARISON_GT"
      threshold_value = 5
      aggregations {
        alignment_period   = "300s"
        per_series_aligner = "ALIGN_SUM"
      }
    }
  }

  notification_channels = [google_monitoring_notification_channel.email.name]
}
