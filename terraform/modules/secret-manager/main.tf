locals {
  secrets = [
    "openai-api-key",
    "anthropic-api-key",
    "servicenow-password",
    "servicenow-url",
    "servicenow-username",
    "jira-api-token",
    "jira-base-url",
    "jira-user-email",
    "github-token",
    "github-org",
    "langfuse-secret-key",
    "langfuse-public-key",
    "langfuse-host",
    "neo4j-password",
    "weaviate-api-key",
    "gcp-project-id",
    "slack-webhook-url",
    "airflow-api-key",
    "database-url",
    "model-armor-template-id",
  ]
}

resource "google_secret_manager_secret" "secrets" {
  for_each  = toset(local.secrets)
  project   = var.project_id
  secret_id = "${each.key}-${var.env}"

  replication {
    auto {}
  }
}

# Grant backend SA access to all secrets
resource "google_secret_manager_secret_iam_member" "backend_accessor" {
  for_each  = toset(local.secrets)
  project   = var.project_id
  secret_id = google_secret_manager_secret.secrets[each.key].secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${var.backend_sa}"
}

# Grant worker SA access to all secrets
resource "google_secret_manager_secret_iam_member" "worker_accessor" {
  for_each  = toset(local.secrets)
  project   = var.project_id
  secret_id = google_secret_manager_secret.secrets[each.key].secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${var.worker_sa}"
}
