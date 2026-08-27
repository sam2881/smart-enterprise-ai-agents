resource "google_redis_instance" "cache" {
  name               = "ai-agent-redis-${var.env}"
  tier               = var.env == "prod" ? "STANDARD_HA" : "BASIC"
  memory_size_gb     = var.env == "prod" ? 5 : 1
  region             = var.region
  project            = var.project_id
  redis_version      = "REDIS_7_0"
  authorized_network = var.vpc_self_link
  connect_mode       = "PRIVATE_SERVICE_ACCESS"

  maintenance_policy {
    weekly_maintenance_window {
      day = "SUNDAY"
      start_time { hours = 3; minutes = 0; seconds = 0; nanos = 0 }
    }
  }

  depends_on = [var.private_service_access_connection]
}
