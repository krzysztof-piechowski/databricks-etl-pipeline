# ===================================================================
# Databricks ETL Pipeline – Azure Infrastructure (Enhanced)
# ===================================================================

# -----------------------------
# Data Sources
# -----------------------------
data "azurerm_subscription" "current" {}

data "azurerm_client_config" "current" {}


# -----------------------------
# Resource Group
# -----------------------------
resource "azurerm_resource_group" "rg" {
  name     = var.resource_group_name
  location = var.location
  
  tags = {
    environment = "dev"
    project     = "databricks-etl-pipeline"
    managed_by  = "terraform"
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

  # Enhanced security settings
  min_tls_version                 = "TLS1_2"
  allow_nested_items_to_be_public = false
  
  # Enable blob versioning and soft delete for data protection
  blob_properties {
    versioning_enabled = true
    
    delete_retention_policy {
      days = 7
    }
    
    container_delete_retention_policy {
      days = 7
    }
  }

  tags = {
    environment = "dev"
    project     = "databricks-etl-pipeline"
    managed_by  = "terraform"
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
# Storage Lifecycle Management Policy
# -----------------------------
resource "azurerm_storage_management_policy" "lifecycle" {
  count              = var.enable_lifecycle_policy ? 1 : 0
  storage_account_id = azurerm_storage_account.storage.id

  rule {
    name    = "delete-old-bronze-data"
    enabled = true
    
    filters {
      prefix_match = ["bronze/customers/", "bronze/orders/", "bronze/products/"]
      blob_types   = ["blockBlob"]
    }
    
    actions {
      base_blob {
        delete_after_days_since_modification_greater_than = var.bronze_retention_days
      }
      
      snapshot {
        delete_after_days_since_creation_greater_than = 30
      }
    }
  }
  
  rule {
    name    = "archive-old-source-data"
    enabled = true
    
    filters {
      prefix_match = ["source/"]
      blob_types   = ["blockBlob"]
    }
    
    actions {
      base_blob {
        tier_to_cool_after_days_since_modification_greater_than    = 30
        tier_to_archive_after_days_since_modification_greater_than = 90
      }
    }
  }
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
    managed_by  = "terraform"
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
    managed_by  = "terraform"
  }
}

# -----------------------------
# Role Assignments (Access to Storage)
# -----------------------------
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
# Azure Key Vault (Secure Storage for Secrets)
# ===================================================================
resource "random_string" "kv_suffix" {
  length  = 4
  special = false
  upper   = false
}

resource "azurerm_key_vault" "kv" {
  name                       = "${var.key_vault_name}-${random_string.kv_suffix.result}"
  location                   = azurerm_resource_group.rg.location
  resource_group_name        = azurerm_resource_group.rg.name
  tenant_id                  = data.azurerm_client_config.current.tenant_id
  sku_name                   = "standard"

  rbac_authorization_enabled      = true
    
  # Security settings
  soft_delete_retention_days = 7
  purge_protection_enabled   = false
  
  # Network settings
  network_acls {
    default_action = "Allow"
    bypass         = "AzureServices"
  }

  tags = {
    environment = "dev"
    project     = "databricks-etl-pipeline"
    managed_by  = "terraform"
  }
}


# -----------------------------
# Role Assignment #1: Current User (for Terraform operations)
# -----------------------------
resource "azurerm_role_assignment" "kv_secret_officer" {
  scope                = azurerm_key_vault.kv.id
  role_definition_name = "Key Vault Secrets Officer"
  principal_id         = data.azurerm_client_config.current.object_id
}

# -----------------------------
# Role Assignment #2: Databricks Access Connector (for reading secrets)
# -----------------------------
resource "azurerm_role_assignment" "kv_secrets_user_connector" {
  scope                = azurerm_key_vault.kv.id
  role_definition_name = "Key Vault Secrets User"
  principal_id         = azurerm_databricks_access_connector.connector.identity[0].principal_id
  depends_on           = [azurerm_databricks_access_connector.connector]
}