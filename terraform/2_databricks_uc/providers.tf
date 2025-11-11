terraform {
  required_version = ">= 1.2"
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.30.0"
    }
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.115"
    }
    time = {
      source  = "hashicorp/time"
      version = "~> 0.9"
  }
  }
}

provider "azurerm" {
  features {}
}

resource "time_sleep" "wait_for_rbac" {
  create_duration = "180s"
}

# -----------------------------
# Import remote state from 1_azure_infra module
# -----------------------------
data "terraform_remote_state" "azure" {
  backend = "local"
  config = {
    path = "../1_azure_infra/terraform.tfstate"
  }
}

# -----------------------------
# Key Vault from previous module
# -----------------------------
data "azurerm_key_vault" "kv" {
  name                = data.terraform_remote_state.azure.outputs.key_vault_name
  resource_group_name = data.terraform_remote_state.azure.outputs.resource_group_name
}

# -----------------------------
# Save Databricks token to Key Vault
# -----------------------------
resource "azurerm_key_vault_secret" "databricks_token" {
  name         = "databricks-token"
  value        = var.databricks_token
  key_vault_id = data.azurerm_key_vault.kv.id

  depends_on = [
    data.terraform_remote_state.azure,
    time_sleep.wait_for_rbac,
    data.azurerm_key_vault.kv
  ]
}

# -----------------------------
# Retrieve Databricks token from Key Vault
# -----------------------------
data "azurerm_key_vault_secret" "token" {
  name         = azurerm_key_vault_secret.databricks_token.name
  key_vault_id = data.azurerm_key_vault.kv.id
  depends_on   = [azurerm_key_vault_secret.databricks_token]
}

# -----------------------------
# Databricks Provider Configuration
# -----------------------------
provider "databricks" {
  host  = "https://${data.terraform_remote_state.azure.outputs.databricks_workspace_url}"
  token = data.azurerm_key_vault_secret.token.value
}