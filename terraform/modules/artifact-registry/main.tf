resource "google_artifact_registry_repository" "repo" {
  location      = var.region
  repository_id = "ai-agent-platform"
  format        = "DOCKER"
  project       = var.project_id
  description   = "AI Agent Platform Docker image registry"
}

# Allow the worker SA to pull images (consumers, MCP servers)
resource "google_artifact_registry_repository_iam_member" "worker_reader" {
  project    = var.project_id
  location   = var.region
  repository = google_artifact_registry_repository.repo.name
  role       = "roles/artifactregistry.reader"
  member     = "serviceAccount:${var.worker_sa}"
}
