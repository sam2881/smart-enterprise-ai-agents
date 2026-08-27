resource "google_sql_database_instance" "postgres" {
  name                = "ai-agent-pg-${var.env}"
  database_version    = "POSTGRES_15"
  region              = var.region
  project             = var.project_id
  deletion_protection = var.env == "prod"

  settings {
    tier              = var.db_tier
    availability_type = var.env == "prod" ? "REGIONAL" : "ZONAL"
    disk_autoresize   = true
    disk_size         = 20

    ip_configuration {
      ipv4_enabled                                  = false
      private_network                               = var.vpc_self_link
      enable_private_path_for_google_cloud_services = true
    }

    backup_configuration {
      enabled                        = var.env == "prod"
      point_in_time_recovery_enabled = var.env == "prod"
      start_time                     = "03:00"
    }

    database_flags {
      name  = "max_connections"
      value = "100"
    }
  }

  depends_on = [var.private_service_access_connection]
}

resource "google_sql_database" "agentdb" {
  instance = google_sql_database_instance.postgres.name
  name     = "agentdb"
  project  = var.project_id
}

resource "google_sql_database" "langfuse" {
  instance = google_sql_database_instance.postgres.name
  name     = "langfuse"
  project  = var.project_id
}

resource "google_sql_database" "airflow" {
  instance = google_sql_database_instance.postgres.name
  name     = "airflow"
  project  = var.project_id
}

resource "google_sql_user" "app_user" {
  instance = google_sql_database_instance.postgres.name
  name     = "agentuser"
  password = var.db_password
  project  = var.project_id
}
