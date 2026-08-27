variable "project_id"       { type = string }
variable "region"           { type = string }
variable "env"              { type = string }
variable "ar_repo"          { type = string }
variable "image_tag"        { type = string; default = "latest" }
variable "vpc_connector_id" { type = string }
variable "backend_sa"       { type = string }
variable "worker_sa"        { type = string }
