# ===================================================================
# Databricks ETL Pipeline – Unity Catalog Setup
# ===================================================================

# Get current Databricks workspace info
data "databricks_current_user" "me" {}

# -----------------------------
# Unity Catalog Metastore
# -----------------------------

locals {
  region_mapping = {
    "Switzerland North" = "switzerlandnorth"
  }
}

resource "databricks_metastore" "main" {
  name          = var.metastore_name
  storage_root  = "abfss://metastore@${data.terraform_remote_state.azure.outputs.storage_account_name}.dfs.core.windows.net/"
  force_destroy = var.force_destroy_metastore
  
  # Use the region from Azure deployment
  region = lookup(local.region_mapping, data.terraform_remote_state.azure.outputs.resource_group_location, lower(replace(data.terraform_remote_state.azure.outputs.resource_group_location, " ", "")))
}

# -----------------------------
# Metastore Data Access (Azure Managed Identity)
# -----------------------------
resource "databricks_metastore_data_access" "azure_managed_identity" {
  metastore_id = databricks_metastore.main.id
  name         = data.terraform_remote_state.azure.outputs.access_connector_name
  
  azure_managed_identity {
    access_connector_id = data.terraform_remote_state.azure.outputs.access_connector_id
  }
  
  is_default = true
  
  depends_on = [databricks_metastore.main]
}

# -----------------------------
# Workspace Metastore Assignment
# -----------------------------
resource "databricks_metastore_assignment" "workspace_binding" {
  workspace_id = data.terraform_remote_state.azure.outputs.databricks_workspace_id
  metastore_id = databricks_metastore.main.id
  
  depends_on = [
    databricks_metastore_data_access.azure_managed_identity
  ]
}

resource "databricks_default_namespace_setting" "this" {
  namespace {
    value = var.catalog_name
  }
  
  depends_on = [
    databricks_metastore_assignment.workspace_binding,
    databricks_catalog.main,
    databricks_schema.bronze,
    databricks_schema.silver,
    databricks_schema.gold
  ]
}

# -----------------------------
# Unity Catalog
# -----------------------------
resource "databricks_catalog" "main" {
  name         = var.catalog_name
  comment      = "Main catalog for Databricks ETL pipeline"
  metastore_id = databricks_metastore.main.id
  
  properties = {
    purpose = "etl-pipeline"
  }
  
  depends_on = [databricks_metastore_assignment.workspace_binding]
}

# Grant permissions to current user
resource "databricks_grants" "catalog_grants" {
  catalog = databricks_catalog.main.name
  
  grant {
    principal  = data.databricks_current_user.me.user_name
    privileges = ["ALL_PRIVILEGES"]
  }

  depends_on = [databricks_catalog.main]
}

# -----------------------------
# Schemas (Bronze, Silver, Gold)
# -----------------------------
resource "databricks_schema" "bronze" {
  catalog_name = databricks_catalog.main.name
  name         = "bronze"
  comment      = "Bronze layer - raw data ingestion"
  
  properties = {
    layer = "bronze"
  }
  
  depends_on = [databricks_catalog.main]
}

resource "databricks_schema" "silver" {
  catalog_name = databricks_catalog.main.name
  name         = "silver"
  comment      = "Silver layer - cleaned and transformed data"
  
  properties = {
    layer = "silver"
  }
  
  depends_on = [databricks_catalog.main]
}

resource "databricks_schema" "gold" {
  catalog_name = databricks_catalog.main.name
  name         = "gold"
  comment      = "Gold layer - business-level aggregated data"
  
  properties = {
    layer = "gold"
  }
  
  depends_on = [databricks_catalog.main]
}

# Grant schema permissions
resource "databricks_grants" "bronze_grants" {
  schema = databricks_schema.bronze.id
  
  grant {
    principal  = data.databricks_current_user.me.user_name
    privileges = ["ALL_PRIVILEGES"]
  }

  depends_on = [
    databricks_schema.bronze
  ]
}

resource "databricks_grants" "silver_grants" {
  schema = databricks_schema.silver.id
  
  grant {
    principal  = data.databricks_current_user.me.user_name
    privileges = ["ALL_PRIVILEGES"]
  }

  depends_on = [
    databricks_schema.silver
  ]
}

resource "databricks_grants" "gold_grants" {
  schema = databricks_schema.gold.id
  
  grant {
    principal  = data.databricks_current_user.me.user_name
    privileges = ["ALL_PRIVILEGES"]
  }
  
  depends_on = [
    databricks_schema.gold
  ]
}

# -----------------------------
# External Locations (for mounting storage)
# -----------------------------
resource "databricks_external_location" "source" {
  name            = "source-location"
  url             = "abfss://source@${data.terraform_remote_state.azure.outputs.storage_account_name}.dfs.core.windows.net/"
  credential_name = databricks_metastore_data_access.azure_managed_identity.name
  comment         = "External location for source data"
  
  depends_on = [
  databricks_metastore_assignment.workspace_binding,
  databricks_metastore_data_access.azure_managed_identity,
  databricks_catalog.main
]
}

resource "databricks_external_location" "bronze" {
  name            = "bronze-location"
  url             = "abfss://bronze@${data.terraform_remote_state.azure.outputs.storage_account_name}.dfs.core.windows.net/"
  credential_name = databricks_metastore_data_access.azure_managed_identity.name
  comment         = "External location for bronze layer"
  
  depends_on = [
  databricks_metastore_assignment.workspace_binding,
  databricks_metastore_data_access.azure_managed_identity,
  databricks_catalog.main
]
}

resource "databricks_external_location" "silver" {
  name            = "silver-location"
  url             = "abfss://silver@${data.terraform_remote_state.azure.outputs.storage_account_name}.dfs.core.windows.net/"
  credential_name = databricks_metastore_data_access.azure_managed_identity.name
  comment         = "External location for silver layer"
  
  depends_on = [
  databricks_metastore_assignment.workspace_binding,
  databricks_metastore_data_access.azure_managed_identity,
  databricks_catalog.main
]
}

resource "databricks_external_location" "gold" {
  name            = "gold-location"
  url             = "abfss://gold@${data.terraform_remote_state.azure.outputs.storage_account_name}.dfs.core.windows.net/"
  credential_name = databricks_metastore_data_access.azure_managed_identity.name
  comment         = "External location for gold layer"
  
  depends_on = [
  databricks_metastore_assignment.workspace_binding,
  databricks_metastore_data_access.azure_managed_identity,
  databricks_catalog.main
]
}
