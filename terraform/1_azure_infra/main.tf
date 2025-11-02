# ===================================================================
# Databricks ETL Pipeline – Azure Infrastructure (with Key Vault)
# ===================================================================

# -----------------------------
# Resource Group
# -----------------------------
resource "azurerm_resource_group" "rg" {
  name     = var.resource_group_name
  location = var.location
  
  tags = {
    environment = "dev"
    project     = "databricks-etl-pipeline"
  }
}

# -----------------------------
# Storage Account (ADLS Gen2)
# -----------------------------
resource "azurerm_storage_account" "storage" {
  name                     = var.storage_account_name
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  is_hns_enabled           = true

  tags = {
    environment = "dev"
    project     = "databricks-etl-pipeline"
  }
}

# -----------------------------
# Storage Containers
# -----------------------------
resource "azurerm_storage_container" "source" {
  name                  = "source"
  storage_account_id    = azurerm_storage_account.storage.id
  container_access_type = "private"
}

resource "azurerm_storage_container" "bronze" {
  name                  = "bronze"
  storage_account_id    = azurerm_storage_account.storage.id
  container_access_type = "private"
}

resource "azurerm_storage_container" "silver" {
  name                  = "silver"
  storage_account_id    = azurerm_storage_account.storage.id
  container_access_type = "private"
}

resource "azurerm_storage_container" "gold" {
  name                  = "gold"
  storage_account_id    = azurerm_storage_account.storage.id
  container_access_type = "private"
}

resource "azurerm_storage_container" "metastore" {
  name                  = "metastore"
  storage_account_id    = azurerm_storage_account.storage.id
  container_access_type = "private"
}

# -----------------------------
# Databricks Workspace
# -----------------------------
resource "azurerm_databricks_workspace" "databricks" {
  name                = var.databricks_workspace_name
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  sku                 = "standard"

  tags = {
    environment = "dev"
    project     = "databricks-etl-pipeline"
  }
}

# -----------------------------
# Access Connector for Databricks
# -----------------------------
resource "azurerm_databricks_access_connector" "connector" {
  name                = var.databricks_connector_name
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  identity {
    type = "SystemAssigned"
  }

  tags = {
    environment = "dev"
    project     = "databricks-etl-pipeline"
  }
}

# -----------------------------
# Role assignment (Access to Storage)
# -----------------------------
data "azurerm_subscription" "current" {}

resource "azurerm_role_assignment" "storage_blob_data_contributor" {
  depends_on           = [azurerm_databricks_access_connector.connector, azurerm_storage_account.storage]
  principal_id         = azurerm_databricks_access_connector.connector.identity[0].principal_id
  role_definition_name = "Storage Blob Data Contributor"
  scope                = azurerm_storage_account.storage.id
}

resource "azurerm_role_assignment" "storage_blob_data_owner" {
  depends_on           = [azurerm_databricks_access_connector.connector, azurerm_storage_account.storage]
  principal_id         = azurerm_databricks_access_connector.connector.identity[0].principal_id
  role_definition_name = "Storage Blob Data Owner"
  scope                = azurerm_storage_account.storage.id
}


# ===================================================================
# Azure Key Vault (secure storage for Databricks token)
# ===================================================================

# Create Key Vault
resource "azurerm_key_vault" "kv" {
  name                        = "kv-databricks-pipeline"
  location                    = azurerm_resource_group.rg.location
  resource_group_name         = azurerm_resource_group.rg.name
  tenant_id                   = data.azurerm_subscription.current.tenant_id
  sku_name                    = "standard"

  soft_delete_retention_days  = 7
  purge_protection_enabled    = false

  tags = {
    environment = "dev"
    project     = "databricks-etl-pipeline"
  }
}

# Give current user access to manage secrets in Key Vault
data "azurerm_client_config" "current" {}

resource "azurerm_role_assignment" "kv_secret_officer" {
  scope                = azurerm_key_vault.kv.id
  role_definition_name = "Key Vault Secrets Officer"
  principal_id         = data.azurerm_client_config.current.object_id
}