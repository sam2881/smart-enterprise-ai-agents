locals {
  backend_roles = [
    "roles/run.invoker",
    "roles/cloudsql.client",
    "roles/secretmanager.secretAccessor",
    "roles/pubsub.publisher",
    "roles/pubsub.subscriber",
    "roles/logging.logWriter",
    "roles/cloudtrace.agent",
    "roles/monitoring.metricWriter",
  ]
  worker_roles = [
    "roles/run.invoker",
    "roles/cloudsql.client",
    "roles/secretmanager.secretAccessor",
    "roles/pubsub.publisher",
    "roles/pubsub.subscriber",
    "roles/logging.logWriter",
    "roles/cloudtrace.agent",
    "roles/monitoring.metricWriter",
    "roles/storage.objectAdmin",
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
    "roles/dataproc.editor",
  ]
  composer_roles = [
    "roles/composer.worker",
    "roles/storage.objectAdmin",
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
    "roles/logging.logWriter",
    "roles/monitoring.metricWriter",
  ]
  gke_roles = [
    "roles/container.nodeServiceAccount",
    "roles/storage.objectViewer",
    "roles/logging.logWriter",
    "roles/monitoring.metricWriter",
    "roles/artifactregistry.reader",
  ]
}

# Backend SA — used by stateless APIs (backend-api, data-agent-api, frontend)
resource "google_service_account" "backend" {
  account_id   = "ai-agent-backend-${var.env}"
  display_name = "AI Agent Backend SA (${var.env})"
  project      = var.project_id
}

resource "google_project_iam_member" "backend" {
  for_each = toset(local.backend_roles)
  project  = var.project_id
  role     = each.key
  member   = "serviceAccount:${google_service_account.backend.email}"
}

# Worker SA — used by consumers, MCP servers, and agents
resource "google_service_account" "worker" {
  account_id   = "ai-agent-worker-${var.env}"
  display_name = "AI Agent Worker SA (${var.env})"
  project      = var.project_id
}

resource "google_project_iam_member" "worker" {
  for_each = toset(local.worker_roles)
  project  = var.project_id
  role     = each.key
  member   = "serviceAccount:${google_service_account.worker.email}"
}

# Composer SA — used by Cloud Composer workers
resource "google_service_account" "composer" {
  account_id   = "ai-agent-composer-${var.env}"
  display_name = "AI Agent Composer SA (${var.env})"
  project      = var.project_id
}

resource "google_project_iam_member" "composer" {
  for_each = toset(local.composer_roles)
  project  = var.project_id
  role     = each.key
  member   = "serviceAccount:${google_service_account.composer.email}"
}

# GKE SA — used by GKE nodes
resource "google_service_account" "gke" {
  account_id   = "ai-agent-gke-${var.env}"
  display_name = "AI Agent GKE SA (${var.env})"
  project      = var.project_id
}

resource "google_project_iam_member" "gke" {
  for_each = toset(local.gke_roles)
  project  = var.project_id
  role     = each.key
  member   = "serviceAccount:${google_service_account.gke.email}"
}
