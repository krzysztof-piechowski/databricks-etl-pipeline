# ===================================================================
# Databricks ETL Pipeline – Unity Catalog Setup
# ===================================================================

# -----------------------------
# Unity Catalog Metastore
# -----------------------------
resource "databricks_metastore" "main" {
  name          = "main_metastore"
  storage_root  = "abfss://metastore@${data.terraform_remote_state.azure.outputs.storage_account_name}.dfs.core.windows.net/"
  force_destroy = true
}

# -----------------------------
# Workspace binding (attach metastore)
# -----------------------------
resource "databricks_metastore_data_access" "azure_managed_identity" {
  metastore_id = databricks_metastore.main.id
  name         = "databricks_access_connector"

  azure_managed_identity {
    access_connector_id = data.terraform_remote_state.azure.outputs.access_connector_id
  }
  depends_on = [databricks_metastore.main]
}

resource "databricks_metastore_assignment" "workspace_binding" {
  workspace_id = data.terraform_remote_state.azure.outputs.databricks_workspace_id
  metastore_id = databricks_metastore.main.id

  depends_on = [databricks_metastore_data_access.azure_managed_identity]
}

# -----------------------------
# Create Unity Catalog
# -----------------------------
resource "databricks_catalog" "main" {
  name         = "databricks_cata"
  comment      = "Main catalog for the Databricks UC setup"
  metastore_id = databricks_metastore.main.id

  depends_on = [databricks_metastore_assignment.workspace_binding]
}

# -----------------------------
# Create schema
# -----------------------------
resource "databricks_schema" "bronze" {
  name         = "bronze"
  catalog_name = databricks_catalog.main.name
  comment      = "Bronze schema for raw data"

  depends_on = [databricks_catalog.main]
}

resource "databricks_schema" "silver" {
  name         = "silver"
  catalog_name = databricks_catalog.main.name
  comment      = "Silver schema for processed data"

  depends_on = [databricks_catalog.main]
}

resource "databricks_schema" "gold" {
  name         = "gold"
  catalog_name = databricks_catalog.main.name
  comment      = "Gold schema for curated data"

  depends_on = [databricks_catalog.main]
}