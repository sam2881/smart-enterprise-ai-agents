locals {
  services = {
    "backend-api" = {
      image            = "${var.ar_repo}/backend:${var.image_tag}"
      port             = 8000
      min_instances    = var.env == "prod" ? 1 : 0
      max_instances    = 10
      memory           = "2Gi"
      cpu              = "2"
      no_cpu_throttle  = false
    }
    "data-agent-api" = {
      image            = "${var.ar_repo}/data-agent:${var.image_tag}"
      port             = 8001
      min_instances    = var.env == "prod" ? 1 : 0
      max_instances    = 5
      memory           = "2Gi"
      cpu              = "2"
      no_cpu_throttle  = false
    }
    "frontend" = {
      image            = "${var.ar_repo}/frontend:${var.image_tag}"
      port             = 3001
      min_instances    = var.env == "prod" ? 1 : 0
      max_instances    = 5
      memory           = "512Mi"
      cpu              = "1"
      no_cpu_throttle  = false
    }
    "event-orchestrator" = {
      image            = "${var.ar_repo}/event-orchestrator:${var.image_tag}"
      port             = 8080
      min_instances    = 1
      max_instances    = 3
      memory           = "1Gi"
      cpu              = "1"
      no_cpu_throttle  = true
    }
    "incident-consumer" = {
      image            = "${var.ar_repo}/incident-consumer:${var.image_tag}"
      port             = 8080
      min_instances    = 1
      max_instances    = 5
      memory           = "1Gi"
      cpu              = "1"
      no_cpu_throttle  = true
    }
    "jira-consumer" = {
      image            = "${var.ar_repo}/jira-consumer:${var.image_tag}"
      port             = 8080
      min_instances    = 1
      max_instances    = 3
      memory           = "512Mi"
      cpu              = "1"
      no_cpu_throttle  = true
    }
    "pipeline-consumer" = {
      image            = "${var.ar_repo}/pipeline-consumer:${var.image_tag}"
      port             = 8080
      min_instances    = 1
      max_instances    = 3
      memory           = "1Gi"
      cpu              = "1"
      no_cpu_throttle  = true
    }
    "proactive-monitor" = {
      image            = "${var.ar_repo}/proactive-monitor:${var.image_tag}"
      port             = 8080
      min_instances    = 1
      max_instances    = 2
      memory           = "512Mi"
      cpu              = "1"
      no_cpu_throttle  = true
    }
    "post-mortem-agent" = {
      image            = "${var.ar_repo}/post-mortem-agent:${var.image_tag}"
      port             = 8080
      min_instances    = 0
      max_instances    = 3
      memory           = "512Mi"
      cpu              = "1"
      no_cpu_throttle  = false
    }
    "mcp-servicenow" = {
      image            = "${var.ar_repo}/mcp-servicenow:${var.image_tag}"
      port             = 8080
      min_instances    = 1
      max_instances    = 3
      memory           = "256Mi"
      cpu              = "1"
      no_cpu_throttle  = false
    }
    "mcp-jira" = {
      image            = "${var.ar_repo}/mcp-jira:${var.image_tag}"
      port             = 8080
      min_instances    = 1
      max_instances    = 3
      memory           = "256Mi"
      cpu              = "1"
      no_cpu_throttle  = false
    }
    "mcp-github" = {
      image            = "${var.ar_repo}/mcp-github:${var.image_tag}"
      port             = 8092
      min_instances    = 1
      max_instances    = 3
      memory           = "256Mi"
      cpu              = "1"
      no_cpu_throttle  = false
    }
    "mcp-airflow" = {
      image            = "${var.ar_repo}/mcp-airflow:${var.image_tag}"
      port             = 8006
      min_instances    = 1
      max_instances    = 3
      memory           = "256Mi"
      cpu              = "1"
      no_cpu_throttle  = false
    }
    "mcp-rag" = {
      image            = "${var.ar_repo}/mcp-rag:${var.image_tag}"
      port             = 8080
      min_instances    = 1
      max_instances    = 5
      memory           = "512Mi"
      cpu              = "1"
      no_cpu_throttle  = false
    }
    "mcp-gcs" = {
      image            = "${var.ar_repo}/mcp-gcs:${var.image_tag}"
      port             = 8011
      min_instances    = 1
      max_instances    = 5
      memory           = "256Mi"
      cpu              = "1"
      no_cpu_throttle  = false
    }
    "mcp-iceberg" = {
      image            = "${var.ar_repo}/mcp-iceberg:${var.image_tag}"
      port             = 8012
      min_instances    = 1
      max_instances    = 3
      memory           = "256Mi"
      cpu              = "1"
      no_cpu_throttle  = false
    }
    "mcp-llm" = {
      image            = "${var.ar_repo}/mcp-llm:${var.image_tag}"
      port             = 8013
      min_instances    = 1
      max_instances    = 5
      memory           = "512Mi"
      cpu              = "1"
      no_cpu_throttle  = false
    }
  }
}

resource "google_cloud_run_v2_service" "services" {
  for_each = local.services
  name     = "${each.key}-${var.env}"
  location = var.region
  project  = var.project_id

  ingress = each.key == "frontend" || each.key == "backend-api" || each.key == "data-agent-api" ? "INGRESS_TRAFFIC_ALL" : "INGRESS_TRAFFIC_INTERNAL_ONLY"

  template {
    service_account = each.value.no_cpu_throttle ? var.worker_sa : var.backend_sa

    scaling {
      min_instance_count = each.value.min_instances
      max_instance_count = each.value.max_instances
    }

    vpc_access {
      connector = var.vpc_connector_id
      egress    = "PRIVATE_RANGES_ONLY"
    }

    containers {
      image = each.value.image

      ports {
        container_port = each.value.port
      }

      resources {
        limits = {
          memory = each.value.memory
          cpu    = each.value.cpu
        }
        cpu_idle          = !each.value.no_cpu_throttle
        startup_cpu_boost = true
      }

      env {
        name  = "ENVIRONMENT"
        value = var.env
      }
      env {
        name  = "GCP_PROJECT_ID"
        value = var.project_id
      }
      env {
        name  = "PUBSUB_ENABLED"
        value = "true"
      }
      env {
        name  = "GOOGLE_CLOUD_PROJECT"
        value = var.project_id
      }

      liveness_probe {
        http_get {
          path = "/health"
          port = each.value.port
        }
        initial_delay_seconds = 10
        period_seconds        = 30
      }
    }
  }
}

# Allow unauthenticated access for public-facing services
resource "google_cloud_run_v2_service_iam_member" "public_access" {
  for_each = toset(["frontend", "backend-api", "data-agent-api"])
  project  = var.project_id
  location = var.region
  name     = google_cloud_run_v2_service.services[each.key].name
  role     = "roles/run.invoker"
  member   = "allUsers"
}

# Internal services: allow backend and worker SAs to invoke
resource "google_cloud_run_v2_service_iam_member" "internal_invoker" {
  for_each = {
    for k, v in local.services : k => v
    if !contains(["frontend", "backend-api", "data-agent-api"], k)
  }
  project  = var.project_id
  location = var.region
  name     = google_cloud_run_v2_service.services[each.key].name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${var.worker_sa}"
}
