resource "google_container_cluster" "cluster" {
  name     = "ai-agent-cluster-${var.env}"
  location = var.region
  project  = var.project_id

  # Autopilot manages node pools automatically
  enable_autopilot = true
  deletion_protection = var.env == "prod"

  network    = var.vpc_name
  subnetwork = var.subnet_name

  ip_allocation_policy {
    cluster_secondary_range_name  = "pods"
    services_secondary_range_name = "services"
  }

  private_cluster_config {
    enable_private_nodes    = true
    enable_private_endpoint = false
    master_ipv4_cidr_block  = "172.16.0.0/28"
  }

  release_channel {
    channel = "REGULAR"
  }

  workload_identity_config {
    workload_pool = "${var.project_id}.svc.id.goog"
  }

  logging_config {
    enable_components = ["SYSTEM_COMPONENTS", "WORKLOADS"]
  }

  monitoring_config {
    enable_components = ["SYSTEM_COMPONENTS"]
  }
}

# Kubernetes service account for workload identity
resource "google_service_account_iam_member" "gke_workload_identity" {
  service_account_id = "projects/${var.project_id}/serviceAccounts/${var.gke_sa}"
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${var.project_id}.svc.id.goog[ai-agent/ai-agent-sa]"
}
