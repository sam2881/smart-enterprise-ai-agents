resource "google_compute_network" "vpc" {
  name                    = "ai-agent-vpc-${var.env}"
  auto_create_subnetworks = false
  project                 = var.project_id
}

resource "google_compute_subnetwork" "subnet" {
  name          = "ai-agent-subnet-${var.env}"
  ip_cidr_range = "10.0.0.0/20"
  region        = var.region
  network       = google_compute_network.vpc.id
  project       = var.project_id

  private_ip_google_access = true

  secondary_ip_range {
    range_name    = "pods"
    ip_cidr_range = "10.4.0.0/14"
  }
  secondary_ip_range {
    range_name    = "services"
    ip_cidr_range = "10.8.0.0/20"
  }
}

# Private service access for Cloud SQL and Memorystore
resource "google_compute_global_address" "private_services" {
  name          = "ai-agent-private-services-${var.env}"
  purpose       = "VPC_PEERING"
  address_type  = "INTERNAL"
  prefix_length = 16
  network       = google_compute_network.vpc.id
  project       = var.project_id
}

resource "google_service_networking_connection" "private_vpc_connection" {
  network                 = google_compute_network.vpc.id
  service                 = "servicenetworking.googleapis.com"
  reserved_peering_ranges = [google_compute_global_address.private_services.name]
}

# VPC Access Connector — lets Cloud Run services reach private VPC resources
resource "google_vpc_access_connector" "connector" {
  name          = "ai-agent-connector-${var.env}"
  region        = var.region
  project       = var.project_id
  network       = google_compute_network.vpc.name
  ip_cidr_range = "10.9.0.0/28"
  min_instances = 2
  max_instances = 3
  machine_type  = "e2-micro"
}

# Cloud Router + NAT for GKE pods that need outbound internet access
resource "google_compute_router" "router" {
  name    = "ai-agent-router-${var.env}"
  region  = var.region
  network = google_compute_network.vpc.id
  project = var.project_id
}

resource "google_compute_router_nat" "nat" {
  name                               = "ai-agent-nat-${var.env}"
  router                             = google_compute_router.router.name
  region                             = var.region
  project                            = var.project_id
  nat_ip_allocate_option             = "AUTO_ONLY"
  source_subnetwork_ip_ranges_to_nat = "ALL_SUBNETWORKS_ALL_IP_RANGES"
}

# Firewall: allow internal traffic within VPC
resource "google_compute_firewall" "allow_internal" {
  name    = "ai-agent-allow-internal-${var.env}"
  network = google_compute_network.vpc.name
  project = var.project_id

  allow {
    protocol = "tcp"
    ports    = ["0-65535"]
  }
  allow {
    protocol = "udp"
    ports    = ["0-65535"]
  }
  allow {
    protocol = "icmp"
  }
  source_ranges = ["10.0.0.0/8"]
}
