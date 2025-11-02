variable "databricks_token" {
  description = "Personal Access Token for Databricks (generated after workspace creation)"
  type        = string
  sensitive   = true
}