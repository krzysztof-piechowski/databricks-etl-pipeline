# ===================================================================
# Outputs - Databricks Unity Catalog Resources
# ===================================================================

output "metastore_id" {
  description = "ID of the Unity Catalog metastore"
  value       = databricks_metastore.main.id
}

output "metastore_name" {
  description = "Name of the Unity Catalog metastore"
  value       = databricks_metastore.main.name
}

output "metastore_storage_root" {
  description = "Storage root location for the metastore"
  value       = databricks_metastore.main.storage_root
}

output "workspace_binding_id" {
  description = "ID of the workspace metastore assignment"
  value       = databricks_metastore_assignment.workspace_binding.id
}

output "main_catalog" {
  description = "Name of the main Unity Catalog"
  value       = databricks_catalog.main.name
}

output "catalog_id" {
  description = "ID of the main catalog"
  value       = databricks_catalog.main.id
}

output "bronze_schema" {
  description = "Full name of the bronze schema"
  value       = "${databricks_catalog.main.name}.${databricks_schema.bronze.name}"
}

output "silver_schema" {
  description = "Full name of the silver schema"
  value       = "${databricks_catalog.main.name}.${databricks_schema.silver.name}"
}

output "gold_schema" {
  description = "Full name of the gold schema"
  value       = "${databricks_catalog.main.name}.${databricks_schema.gold.name}"
}

output "external_locations" {
  description = "External locations for data access"
  value = {
    source = databricks_external_location.source.url
    bronze = databricks_external_location.bronze.url
    silver = databricks_external_location.silver.url
    gold   = databricks_external_location.gold.url
  }
}

output "current_user" {
  description = "Current Databricks user"
  value       = data.databricks_current_user.me.user_name
}

# ===================================================================
# Usage Instructions
# ===================================================================

output "usage_instructions" {
  description = "Instructions for using Unity Catalog"
  value = <<-EOT
    ===================================================================
    UNITY CATALOG SETUP COMPLETE! (2/2)
    ===================================================================
    
    ✅ METASTORE:
    - Name: ${databricks_metastore.main.name}
    - ID: ${databricks_metastore.main.id}
    - Storage Root: ${databricks_metastore.main.storage_root}
    - Region: ${data.terraform_remote_state.azure.outputs.resource_group_location}
    
    ✅ CATALOG: ${databricks_catalog.main.name}
    
    ✅ SCHEMAS (Full Qualified Names):
    - ${databricks_catalog.main.name}.bronze  (raw data ingestion)
    - ${databricks_catalog.main.name}.silver  (cleaned & transformed)
    - ${databricks_catalog.main.name}.gold    (business aggregates)
    
    ✅ EXTERNAL LOCATIONS:
    - source-location  → ${databricks_external_location.source.url}
    - bronze-location  → ${databricks_external_location.bronze.url}
    - silver-location  → ${databricks_external_location.silver.url}
    - gold-location    → ${databricks_external_location.gold.url}
    
    ✅ SECURITY:
    - Databricks token stored in Key Vault: ${data.azurerm_key_vault.kv.name}
    - Access Connector configured with managed identity
    - Current user (${data.databricks_current_user.me.user_name}) has ALL_PRIVILEGES
       
    ===================================================================
  EOT
}