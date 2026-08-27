output "airflow_uri"   { value = google_composer_environment.composer.config[0].airflow_uri }
output "gcs_bucket"   { value = google_composer_environment.composer.config[0].dag_gcs_prefix }
output "environment_name" { value = google_composer_environment.composer.name }
