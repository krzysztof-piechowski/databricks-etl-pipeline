output "resource_group_name" {
  value = azurerm_resource_group.rg.name
}

output "storage_account_name" {
  value = azurerm_storage_account.storage.name
}

output "databricks_workspace_id" {
  value = azurerm_databricks_workspace.databricks.id
}

output "databricks_workspace_url" {
  value = azurerm_databricks_workspace.databricks.workspace_url
}

output "access_connector_id" {
  value = azurerm_databricks_access_connector.connector.id
}

output "key_vault_name" {
  value = azurerm_key_vault.kv.name
}

output "key_vault_id" {
  value = azurerm_key_vault.kv.id
}
