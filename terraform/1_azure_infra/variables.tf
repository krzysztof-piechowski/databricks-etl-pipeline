variable "location" {
  description = "Azure region where all resources will be deployed"
  type        = string
  default     = "Switzerland North"
}

variable "resource_group_name" {
  description = "Name of the resource group"
  type        = string
  default     = "RG-Databricks-project"
  
  validation {
    condition     = can(regex("^[a-zA-Z0-9-_()]+$", var.resource_group_name))
    error_message = "Resource group name can only contain alphanumeric characters, hyphens, underscores, and parentheses."
  }
}

variable "storage_account_name" {
  description = "Name of the Azure Storage Account (must be globally unique, 3-24 lowercase alphanumeric characters)"
  type        = string
  default     = "storageaccpiechk"
  
  validation {
    condition     = length(var.storage_account_name) >= 3 && length(var.storage_account_name) <= 24 && can(regex("^[a-z0-9]+$", var.storage_account_name))
    error_message = "Storage account name must be between 3 and 24 characters and can only contain lowercase letters and numbers."
  }
}

variable "databricks_workspace_name" {
  description = "Name of the Databricks Workspace (alphanumeric and hyphens only)"
  type        = string
  default     = "databricks-ws"
  
  validation {
    condition     = can(regex("^[a-zA-Z0-9-]+$", var.databricks_workspace_name))
    error_message = "Databricks workspace name can only contain alphanumeric characters and hyphens."
  }
}

variable "databricks_connector_name" {
  description = "Name of the Databricks Access Connector (alphanumeric and hyphens only)"
  type        = string
  default     = "databricks-con"
  
  validation {
    condition     = can(regex("^[a-zA-Z0-9-]+$", var.databricks_connector_name))
    error_message = "Databricks connector name can only contain alphanumeric characters and hyphens."
  }
}

variable "key_vault_name" {
  description = "Key Vault name"
  type        = string
  default     = "kv-dbr-pipe"
  
  validation {
    condition     = length(var.key_vault_name) <= 24
    error_message = "Key Vault name must be 24 characters or less."
  }
}

variable "enable_lifecycle_policy" {
  description = "Enable lifecycle management policy for storage account"
  type        = bool
  default     = true
}

variable "bronze_retention_days" {
  description = "Number of days to retain data in bronze container before deletion"
  type        = number
  default     = 90
}