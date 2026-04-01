# =============================================================================
# OUTPUTS — Exporta nombres, URLs y ARNs para el pipeline
# =============================================================================

output "resource_group_name" {
  value = azurerm_resource_group.main.name
}

output "storage_account_name" {
  value = azurerm_storage_account.datalake.name
}

output "storage_account_primary_dfs_endpoint" {
  value = azurerm_storage_account.datalake.primary_dfs_endpoint
}

output "databricks_workspace_url" {
  value = azurerm_databricks_workspace.main.workspace_url
}

output "databricks_workspace_id" {
  value = azurerm_databricks_workspace.main.workspace_id
}

output "data_factory_name" {
  value = azurerm_data_factory.main.name
}

output "data_factory_id" {
  value = azurerm_data_factory.main.id
}

output "key_vault_uri" {
  value = azurerm_key_vault.main.vault_uri
}

output "log_analytics_workspace_id" {
  value = azurerm_log_analytics_workspace.main.id
}

#output "sql_server_fqdn" {
#  value = azurerm_mssql_server.source.fully_qualified_domain_name
#}
#
#output "sql_database_name" {
#  value = azurerm_mssql_database.transactional.name
#}

output "action_group_id" {
  value = azurerm_monitor_action_group.pipeline_alerts.id
}

output "container_ids" {
  value = { for k, v in azurerm_storage_container.containers : k => v.id }
}
