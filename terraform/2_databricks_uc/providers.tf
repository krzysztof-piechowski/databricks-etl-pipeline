terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.30.0"
    }
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.115"
    }
  }
}

provider "azurerm" {
  features {}
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
}


# -----------------------------
# Retrieve Databricks token from Key Vault
# -----------------------------
data "azurerm_key_vault_secret" "token" {
  name         = azurerm_key_vault_secret.databricks_token.name
  key_vault_id = data.azurerm_key_vault.kv.id

  depends_on = [azurerm_key_vault_secret.databricks_token]
}


# -----------------------------
# Databricks Provider
# -----------------------------
provider "databricks" {
  host  = data.terraform_remote_state.azure.outputs.databricks_workspace_url
  token = data.azurerm_key_vault_secret.token.value
}
