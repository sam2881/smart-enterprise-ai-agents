resource "google_composer_environment" "composer" {
  name    = "ai-agent-composer-${var.env}"
  region  = var.region
  project = var.project_id

  config {
    software_config {
      image_version = "composer-2.6.6-airflow-2.7.3"

      airflow_config_overrides = {
        "core-dags_are_paused_at_creation" = "true"
        "core-max_active_runs_per_dag"     = "5"
        "webserver-expose_config"          = "false"
      }

      pypi_packages = {
        "google-cloud-bigquery"    = ">=3.0.0"
        "google-cloud-pubsub"      = ">=2.0.0"
        "google-cloud-storage"     = ">=2.0.0"
        "google-cloud-dataproc"    = ">=5.0.0"
        "structlog"                = ">=23.0.0"
        "apache-airflow-providers-google" = ">=10.0.0"
      }

      env_variables = {
        GCP_PROJECT_ID = var.project_id
        ENVIRONMENT    = var.env
      }
    }

    workloads_config {
      scheduler {
        cpu        = var.env == "prod" ? 2 : 0.5
        memory_gb  = var.env == "prod" ? 4 : 1.875
        storage_gb = 1
        count      = var.env == "prod" ? 2 : 1
      }
      web_server {
        cpu       = 0.5
        memory_gb = 1.875
        storage_gb = 1
      }
      worker {
        cpu        = var.env == "prod" ? 2 : 2
        memory_gb  = var.env == "prod" ? 10 : 7.5
        storage_gb = 10
        min_count  = var.env == "prod" ? 2 : 1
        max_count  = var.env == "prod" ? 6 : 3
      }
    }

    environment_size = var.env == "prod" ? "ENVIRONMENT_SIZE_MEDIUM" : "ENVIRONMENT_SIZE_SMALL"

    node_config {
      network         = var.vpc_name
      subnetwork      = var.subnet_name
      service_account = var.composer_sa
    }

    private_environment_config {
      enable_private_endpoint = false
    }
  }
}
