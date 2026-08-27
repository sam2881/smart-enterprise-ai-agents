terraform {
  required_version = ">= 1.5"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
    google-beta = {
      source  = "hashicorp/google-beta"
      version = "~> 5.0"
    }
  }
  backend "gcs" {}
}

provider "google" {
  project = var.project_id
  region  = var.region
}

provider "google-beta" {
  project = var.project_id
  region  = var.region
}

# ── Enable required APIs ───────────────────────────────────────────────────────
resource "google_project_service" "apis" {
  for_each = toset([
    "run.googleapis.com",
    "artifactregistry.googleapis.com",
    "secretmanager.googleapis.com",
    "sqladmin.googleapis.com",
    "redis.googleapis.com",
    "container.googleapis.com",
    "composer.googleapis.com",
    "pubsub.googleapis.com",
    "vpcaccess.googleapis.com",
    "servicenetworking.googleapis.com",
    "monitoring.googleapis.com",
    "cloudtrace.googleapis.com",
    "logging.googleapis.com",
    "iam.googleapis.com",
    "modelarmor.googleapis.com",
  ])
  service            = each.key
  disable_on_destroy = false
}

# ── Modules ────────────────────────────────────────────────────────────────────
module "networking" {
  source     = "./modules/networking"
  project_id = var.project_id
  region     = var.region
  env        = var.environment
  depends_on = [google_project_service.apis]
}

module "iam" {
  source     = "./modules/iam"
  project_id = var.project_id
  env        = var.environment
  depends_on = [google_project_service.apis]
}

module "artifact_registry" {
  source     = "./modules/artifact-registry"
  project_id = var.project_id
  region     = var.region
  worker_sa  = module.iam.worker_sa_email
  depends_on = [google_project_service.apis]
}

module "secret_manager" {
  source     = "./modules/secret-manager"
  project_id = var.project_id
  backend_sa = module.iam.backend_sa_email
  worker_sa  = module.iam.worker_sa_email
  composer_sa = module.iam.composer_sa_email
  depends_on = [google_project_service.apis]
}

module "cloud_sql" {
  source          = "./modules/cloud-sql"
  project_id      = var.project_id
  region          = var.region
  env             = var.environment
  network_id      = module.networking.vpc_id
  depends_on      = [module.networking, google_project_service.apis]
}

module "memorystore" {
  source          = "./modules/memorystore"
  project_id      = var.project_id
  region          = var.region
  env             = var.environment
  network_id      = module.networking.vpc_id
  depends_on      = [module.networking, google_project_service.apis]
}

module "pubsub" {
  source     = "./modules/pubsub"
  project_id = var.project_id
  worker_sa  = module.iam.worker_sa_email
  backend_sa = module.iam.backend_sa_email
  depends_on = [google_project_service.apis]
}

module "gke" {
  source              = "./modules/gke"
  project_id          = var.project_id
  region              = var.region
  env                 = var.environment
  network             = module.networking.vpc_name
  subnetwork          = module.networking.subnet_name
  gke_sa_email        = module.iam.gke_sa_email
  depends_on          = [module.networking, google_project_service.apis]
}

module "cloud_run" {
  source            = "./modules/cloud-run"
  project_id        = var.project_id
  region            = var.region
  env               = var.environment
  vpc_connector_id  = module.networking.vpc_connector_id
  backend_sa_email  = module.iam.backend_sa_email
  worker_sa_email   = module.iam.worker_sa_email
  ar_repo_url       = module.artifact_registry.repo_url
  db_url_secret     = module.secret_manager.postgres_url_secret_id
  redis_url_secret  = module.secret_manager.redis_url_secret_id
  depends_on        = [module.networking, module.iam, module.artifact_registry, module.secret_manager, google_project_service.apis]
}

module "composer" {
  source       = "./modules/composer"
  project_id   = var.project_id
  region       = var.region
  env          = var.environment
  network      = module.networking.vpc_name
  subnetwork   = module.networking.subnet_name
  composer_sa  = module.iam.composer_sa_email
  depends_on   = [module.networking, module.iam, google_project_service.apis]
}

module "monitoring" {
  source      = "./modules/monitoring"
  project_id  = var.project_id
  alert_email = var.alert_email
  depends_on  = [google_project_service.apis]
}
