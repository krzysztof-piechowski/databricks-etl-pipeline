variable "location" {
  description = "Azure region where all resources will be deployed"
  type        = string
  default     = "Switzerland North"
}

variable "resource_group_name" {
  description = "Name of the resource group"
  type        = string
  default     = "RG_Databricks_project"
}

variable "storage_account_name" {
  description = "Name of the Azure Storage Account"
  type        = string
  default     = "storageaccpiechk"
}

variable "databricks_workspace_name" {
  description = "Name of the Databricks Workspace"
  type        = string
  default     = "databricks_ws"
}

variable "databricks_connector_name" {
  description = "Name of the Databricks Access Connector"
  type        = string
  default     = "databricks_con"
}