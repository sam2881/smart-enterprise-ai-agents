output "vpc_id"                            { value = google_compute_network.vpc.id }
output "vpc_name"                          { value = google_compute_network.vpc.name }
output "vpc_self_link"                     { value = google_compute_network.vpc.self_link }
output "subnet_name"                       { value = google_compute_subnetwork.subnet.name }
output "vpc_connector_id"                  { value = google_vpc_access_connector.connector.id }
output "private_service_access_connection" { value = google_service_networking_connection.private_vpc_connection }
