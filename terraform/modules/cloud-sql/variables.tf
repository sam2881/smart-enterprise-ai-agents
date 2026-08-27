variable "project_id"                      { type = string }
variable "region"                          { type = string }
variable "env"                             { type = string }
variable "vpc_self_link"                   { type = string }
variable "private_service_access_connection" { type = any }
variable "db_tier"                         { type = string; default = "db-g1-small" }
variable "db_password"                     { type = string; sensitive = true }
