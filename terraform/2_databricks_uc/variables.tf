variable "databricks_token" {
  description = "Personal Access Token for Databricks (generated after workspace creation)"
  type        = string
  sensitive   = true
  
  validation {
    condition     = length(var.databricks_token) > 0
    error_message = "Databricks token cannot be empty. Generate a token in your Databricks workspace under User Settings > Access Tokens."
  }
}

variable "metastore_name" {
  description = "Name of the Unity Catalog metastore"
  type        = string
  default     = "main-metastore"
}

variable "catalog_name" {
  description = "Name of the Unity Catalog"
  type        = string
  default     = "databricks_cata"
  
  validation {
    condition     = can(regex("^[a-z0-9_-]+$", var.catalog_name))
    error_message = "Catalog name must contain only lowercase letters, numbers, hyphens, and underscores."
  }
}

variable "force_destroy_metastore" {
  description = "Whether to force destroy the metastore when deleting (WARNING: This will delete all data)"
  type        = bool
  default     = true
}