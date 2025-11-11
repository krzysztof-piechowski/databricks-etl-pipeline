# ===================================================================
# Outputs - Resource Information
# ===================================================================

output "resource_group_name" {
  description = "Name of the created resource group"
  value       = azurerm_resource_group.rg.name
}

output "resource_group_location" {
  description = "Location of the resource group"
  value       = azurerm_resource_group.rg.location
}

output "storage_account_name" {
  description = "Name of the storage account"
  value       = azurerm_storage_account.storage.name
}

output "storage_account_id" {
  description = "Resource ID of the storage account"
  value       = azurerm_storage_account.storage.id
}

output "storage_account_primary_endpoint" {
  description = "Primary blob endpoint for the storage account"
  value       = azurerm_storage_account.storage.primary_blob_endpoint
}

output "storage_account_primary_dfs_endpoint" {
  description = "Primary DFS endpoint for ADLS Gen2"
  value       = azurerm_storage_account.storage.primary_dfs_endpoint
}

output "storage_containers" {
  description = "List of created storage containers"
  value = {
    source    = azurerm_storage_container.source.name
    bronze    = azurerm_storage_container.bronze.name
    silver    = azurerm_storage_container.silver.name
    gold      = azurerm_storage_container.gold.name
    metastore = azurerm_storage_container.metastore.name
  }
}

output "databricks_workspace_url" {
  description = "URL of the Databricks workspace"
  value       = azurerm_databricks_workspace.databricks.workspace_url
}

output "databricks_workspace_name" {
  description = "Name of the Databricks workspace"
  value       = azurerm_databricks_workspace.databricks.name
}

output "databricks_workspace_id" {
  description = "Numeric workspace ID for Unity Catalog"
  value       = azurerm_databricks_workspace.databricks.workspace_id
}

output "databricks_workspace_resource_id" {
  description = "Full ARM resource ID of the Databricks workspace"
  value       = azurerm_databricks_workspace.databricks.id
}

output "access_connector_name" {
  description = "Name of the Databricks access connector"
  value       = azurerm_databricks_access_connector.connector.name
}

output "access_connector_id" {
  description = "Resource ID of the Databricks access connector"
  value       = azurerm_databricks_access_connector.connector.id
}

output "access_connector_principal_id" {
  description = "Principal ID of the access connector's managed identity"
  value       = azurerm_databricks_access_connector.connector.identity[0].principal_id
}

output "key_vault_name" {
  description = "Name of the Key Vault"
  value       = azurerm_key_vault.kv.name
}

output "key_vault_id" {
  description = "Resource ID of the Key Vault"
  value       = azurerm_key_vault.kv.id
}

output "key_vault_uri" {
  description = "URI of the Key Vault"
  value       = azurerm_key_vault.kv.vault_uri
}

# ===================================================================
# Connection Strings and Instructions
# ===================================================================

output "next_steps" {
  description = "Next steps for configuring the environment"
  value = <<-EOT
    ===================================================================
    AZURE INFRASTRUCTURE DEPLOYMENT SUCCESSFUL! (1/2)
    ===================================================================
    
    RESOURCE GROUP:
    - Name: ${azurerm_resource_group.rg.name}
    - Location: ${azurerm_resource_group.rg.location}
    
    DATABRICKS WORKSPACE:
    - Name: ${azurerm_databricks_workspace.databricks.name}
    - URL: https://${azurerm_databricks_workspace.databricks.workspace_url}
    - Resource ID: ${azurerm_databricks_workspace.databricks.id}
    
    STORAGE ACCOUNT (ADLS Gen2):
    - Name: ${azurerm_storage_account.storage.name}
    - DFS Endpoint: ${azurerm_storage_account.storage.primary_dfs_endpoint}
    - Containers: source, bronze, silver, gold, metastore
    
    ACCESS CONNECTOR:
    - Name: ${azurerm_databricks_access_connector.connector.name}
    - Principal ID: ${azurerm_databricks_access_connector.connector.identity[0].principal_id}
    - Roles: Storage Blob Data Contributor, Storage Blob Data Owner
    
    KEY VAULT:
    - Name: ${azurerm_key_vault.kv.name}
    - URI: ${azurerm_key_vault.kv.vault_uri}
    - Status: Access policies configured for current user
    
    ===================================================================
    NEXT STEPS - DEPLOY DATABRICKS UNITY CATALOG (2/2):
    ===================================================================
    
    1. Generate Databricks Personal Access Token:
       - Navigate to: https://${azurerm_databricks_workspace.databricks.workspace_url}
       - Go to: User Settings (top right) > Developer > Access Tokens
       - Click "Generate New Token"
       - Name: "Terraform Unity Catalog Setup"
       - Lifetime: 90 days (or as needed)
       - Click "Generate"
       - COPY THE TOKEN - you won't see it again!
    
    2. Deploy Unity Catalog infrastructure:
       cd ../2_databricks_uc
       terraform init
       terraform apply -var="databricks_token=dapi..."
    
    3. The second deployment will:
       - Store your token securely in Key Vault
       - Create Unity Catalog metastore
       - Set up bronze, silver, gold schemas
       - Configure external locations for data access
       - Grant necessary permissions
    
    ===================================================================
    STORAGE PATH EXAMPLES (for reference):
    ===================================================================
    
    Source:    abfss://source@${azurerm_storage_account.storage.name}.dfs.core.windows.net/
    Bronze:    abfss://bronze@${azurerm_storage_account.storage.name}.dfs.core.windows.net/
    Silver:    abfss://silver@${azurerm_storage_account.storage.name}.dfs.core.windows.net/
    Gold:      abfss://gold@${azurerm_storage_account.storage.name}.dfs.core.windows.net/
    Metastore: abfss://metastore@${azurerm_storage_account.storage.name}.dfs.core.windows.net/
    
    ===================================================================
  EOT
}