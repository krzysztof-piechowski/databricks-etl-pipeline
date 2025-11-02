output "metastore_id" {
  value = databricks_metastore.main.id
}

output "workspace_binding_id" {
  value = databricks_metastore_assignment.workspace_binding.id
}

output "main_catalog" {
  value = databricks_catalog.main.name
}

output "bronze_schema" {
  value = databricks_schema.bronze.name
}

output "silver_schema" {
  value = databricks_schema.silver.name
}

output "gold_schema" {
  value = databricks_schema.gold.name
}

output "metastore_storage_root" {
  value = databricks_metastore.main.storage_root
}
