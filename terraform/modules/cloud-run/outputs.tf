output "service_urls" {
  value = { for k, v in google_cloud_run_v2_service.services : k => v.uri }
}

output "frontend_url"    { value = google_cloud_run_v2_service.services["frontend"].uri }
output "backend_url"     { value = google_cloud_run_v2_service.services["backend-api"].uri }
output "data_agent_url"  { value = google_cloud_run_v2_service.services["data-agent-api"].uri }
