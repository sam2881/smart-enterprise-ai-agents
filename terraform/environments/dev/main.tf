variable "project_id"  { type = string }
variable "region"      { type = string }
variable "zone"        { type = string }
variable "environment" { type = string }
variable "alert_email" { type = string }
variable "db_tier"     { type = string; default = "db-g1-small" }
# db_password is injected at apply time via TF_VAR_db_password env var

variable "db_password" {
  type      = string
  sensitive = true
}

provider "google" {
  project = var.project_id
  region  = var.region
}

provider "google-beta" {
  project = var.project_id
  region  = var.region
}

module "networking" {
  source     = "../../modules/networking"
  project_id = var.project_id
  region     = var.region
  env        = var.environment
}

module "iam" {
  source     = "../../modules/iam"
  project_id = var.project_id
  env        = var.environment
}

module "artifact_registry" {
  source     = "../../modules/artifact-registry"
  project_id = var.project_id
  region     = var.region
  worker_sa  = module.iam.worker_sa_email
}

module "secret_manager" {
  source     = "../../modules/secret-manager"
  project_id = var.project_id
  env        = var.environment
  backend_sa = module.iam.backend_sa_email
  worker_sa  = module.iam.worker_sa_email
}

module "cloud_sql" {
  source                           = "../../modules/cloud-sql"
  project_id                       = var.project_id
  region                           = var.region
  env                              = var.environment
  vpc_self_link                    = module.networking.vpc_self_link
  private_service_access_connection = module.networking.private_service_access_connection
  db_tier                          = var.db_tier
  db_password                      = var.db_password
}

module "memorystore" {
  source                           = "../../modules/memorystore"
  project_id                       = var.project_id
  region                           = var.region
  env                              = var.environment
  vpc_self_link                    = module.networking.vpc_self_link
  private_service_access_connection = module.networking.private_service_access_connection
}

module "pubsub" {
  source     = "../../modules/pubsub"
  project_id = var.project_id
  backend_sa = module.iam.backend_sa_email
  worker_sa  = module.iam.worker_sa_email
}

module "gke" {
  source      = "../../modules/gke"
  project_id  = var.project_id
  region      = var.region
  env         = var.environment
  vpc_name    = module.networking.vpc_name
  subnet_name = module.networking.subnet_name
  gke_sa      = module.iam.gke_sa_email
}

module "cloud_run" {
  source           = "../../modules/cloud-run"
  project_id       = var.project_id
  region           = var.region
  env              = var.environment
  ar_repo          = module.artifact_registry.repo_url
  vpc_connector_id = module.networking.vpc_connector_id
  backend_sa       = module.iam.backend_sa_email
  worker_sa        = module.iam.worker_sa_email
}

module "composer" {
  source      = "../../modules/composer"
  project_id  = var.project_id
  region      = var.region
  env         = var.environment
  vpc_name    = module.networking.vpc_name
  subnet_name = module.networking.subnet_name
  composer_sa = module.iam.composer_sa_email
}

module "monitoring" {
  source      = "../../modules/monitoring"
  project_id  = var.project_id
  env         = var.environment
  alert_email = var.alert_email
  backend_url = module.cloud_run.backend_url
}

output "frontend_url"   { value = module.cloud_run.frontend_url }
output "backend_url"    { value = module.cloud_run.backend_url }
output "data_agent_url" { value = module.cloud_run.data_agent_url }
output "composer_url"   { value = module.composer.airflow_uri }
output "gke_cluster"    { value = module.gke.cluster_name }
output "sql_host"       { value = module.cloud_sql.private_ip_address }
output "redis_host"     { value = module.memorystore.host }
output "ar_repo"        { value = module.artifact_registry.repo_url }
