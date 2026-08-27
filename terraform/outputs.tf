output "cloud_run_urls" {
  description = "Map of Cloud Run service name → URL"
  value       = module.cloud_run.service_urls
}

output "gke_cluster_name" {
  description = "GKE cluster name"
  value       = module.gke.cluster_name
}

output "gke_endpoint" {
  description = "GKE cluster API endpoint"
  value       = module.gke.cluster_endpoint
  sensitive   = true
}

output "cloud_sql_connection_name" {
  description = "Cloud SQL connection name for Cloud SQL Auth Proxy"
  value       = module.cloud_sql.connection_name
}

output "cloud_sql_private_ip" {
  description = "Cloud SQL private IP address"
  value       = module.cloud_sql.private_ip_address
}

output "memorystore_host" {
  description = "Memorystore Redis host"
  value       = module.memorystore.host
}

output "composer_airflow_uri" {
  description = "Cloud Composer Airflow web UI URI"
  value       = module.composer.airflow_uri
}

output "composer_dag_bucket" {
  description = "GCS bucket where Composer reads DAGs from"
  value       = module.composer.gcs_bucket
}

output "artifact_registry_url" {
  description = "Artifact Registry repository URL"
  value       = module.artifact_registry.repo_url
}

output "vpc_connector_id" {
  description = "VPC Access Connector ID for Cloud Run → VPC"
  value       = module.networking.vpc_connector_id
}
