# =============================================================================
# FASE 2: Infraestructura como Código — Microsoft Azure
# FinBank End-to-End Data Pipeline
# =============================================================================
# Recursos: Resource Group, ADLS Gen2, Databricks, Key Vault,
#           Data Factory, Log Analytics, Action Group, SQL Database
# =============================================================================

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.85"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.35"
    }
  }

  # Backend remoto — Estado almacenado en Azure Storage (NUNCA en el repo)
  backend "azurerm" {
    resource_group_name  = "rg-terraform-state"
    storage_account_name = "stterraformfinbank"
    container_name       = "tfstate"
    key                  = "finbank-pipeline.terraform.tfstate"
  }
}

provider "azurerm" {
  features {
    key_vault {
      purge_soft_delete_on_destroy = false
    }
  }
}

# =============================================================================
# DATA SOURCES
# =============================================================================
data "azurerm_client_config" "current" {}

# =============================================================================
# RESOURCE GROUP
# =============================================================================
resource "azurerm_resource_group" "main" {
  name     = "${var.resource_prefix}-${var.environment}"
  location = var.location
  tags     = var.tags
}

# =============================================================================
# STORAGE ACCOUNT — ADLS Gen2 (Data Lake)
# =============================================================================
resource "azurerm_storage_account" "datalake" {
  name                     = "${var.storage_account_name}${var.environment}"
  resource_group_name      = azurerm_resource_group.main.name
  location                 = azurerm_resource_group.main.location
  account_tier             = "Standard"
  account_replication_type = var.storage_replication
  account_kind             = "StorageV2"
  is_hns_enabled           = true  # Hierarchical Namespace = ADLS Gen2

  # Seguridad
  min_tls_version                 = "TLS1_2"
  allow_nested_items_to_be_public = false

  blob_properties {
    delete_retention_policy {
      days = 7
    }
    container_delete_retention_policy {
      days = 7
    }
  }

  tags = var.tags
}

# Contenedores del Data Lake (Bronze / Silver / Gold / Landing / Errors / Logs)
resource "azurerm_storage_container" "containers" {
  for_each              = toset(var.datalake_containers)
  name                  = each.value
  storage_account_name  = azurerm_storage_account.datalake.name
  container_access_type = "private"
}

# =============================================================================
# AZURE KEY VAULT — Gestión de Secretos
# =============================================================================
resource "azurerm_key_vault" "main" {
  name                        = "${var.key_vault_name}-${var.environment}"
  resource_group_name         = azurerm_resource_group.main.name
  location                    = azurerm_resource_group.main.location
  tenant_id                   = data.azurerm_client_config.current.tenant_id
  sku_name                    = "standard"
  soft_delete_retention_days  = 7
  purge_protection_enabled    = false
  enable_rbac_authorization   = true

  tags = var.tags
}

# =============================================================================
# AZURE DATABRICKS WORKSPACE
# =============================================================================
resource "azurerm_databricks_workspace" "main" {
  name                        = "${var.databricks_workspace_name}-${var.environment}"
  resource_group_name         = azurerm_resource_group.main.name
  location                    = azurerm_resource_group.main.location
  sku                         = var.databricks_sku
  managed_resource_group_name = "${var.resource_prefix}-dbw-managed-${var.environment}"

  custom_parameters {
    no_public_ip = false
  }

  tags = var.tags
}

# =============================================================================
# AZURE DATA FACTORY
# =============================================================================
resource "azurerm_data_factory" "main" {
  name                = "${var.data_factory_name}-${var.environment}"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location

  identity {
    type = "SystemAssigned"
  }

  tags = var.tags
}

# Linked Service: Data Factory → ADLS Gen2
resource "azurerm_data_factory_linked_service_data_lake_storage_gen2" "datalake" {
  name                 = "ls_adls_datalake"
  data_factory_id      = azurerm_data_factory.main.id
  url                  = "https://${azurerm_storage_account.datalake.name}.dfs.core.windows.net"
  use_managed_identity = true
}

# =============================================================================
# LOG ANALYTICS WORKSPACE + DIAGNOSTICS
# =============================================================================
resource "azurerm_log_analytics_workspace" "main" {
  name                = "${var.log_analytics_name}-${var.environment}"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  sku                 = "PerGB2018"
  retention_in_days   = var.log_retention_days

  tags = var.tags
}

# Diagnostic settings para el Storage Account
resource "azurerm_monitor_diagnostic_setting" "storage" {
  name                       = "diag-storage-datalake"
  target_resource_id         = "${azurerm_storage_account.datalake.id}/blobServices/default"
  log_analytics_workspace_id = azurerm_log_analytics_workspace.main.id

  enabled_log {
    category = "StorageRead"
  }
  enabled_log {
    category = "StorageWrite"
  }
  enabled_log {
    category = "StorageDelete"
  }
  metric {
    category = "Transaction"
  }
}

# =============================================================================
# ACTION GROUP — Alertas por Email
# =============================================================================
resource "azurerm_monitor_action_group" "pipeline_alerts" {
  name                = "ag-finbank-pipeline-${var.environment}"
  resource_group_name = azurerm_resource_group.main.name
  short_name          = "finbank"

  dynamic "email_receiver" {
    for_each = var.alert_email_recipients
    content {
      name          = "alert-${email_receiver.key}"
      email_address = email_receiver.value
    }
  }

  tags = var.tags
}

# =============================================================================
# AZURE SQL DATABASE — Fuente Transaccional
# =============================================================================
resource "azurerm_mssql_server" "source" {
  name                         = "${var.sql_server_name}-${var.environment}"
  resource_group_name          = azurerm_resource_group.main.name
  location                     = azurerm_resource_group.main.location
  version                      = "12.0"
  administrator_login          = var.sql_admin_login
  administrator_login_password = var.sql_admin_password  # Se pasa como variable sensible

  tags = var.tags
}

resource "azurerm_mssql_database" "transactional" {
  name      = var.sql_database_name
  server_id = azurerm_mssql_server.source.id
  sku_name  = var.sql_sku

  tags = var.tags
}

# Firewall: Permitir servicios Azure
resource "azurerm_mssql_firewall_rule" "allow_azure" {
  name             = "AllowAzureServices"
  server_id        = azurerm_mssql_server.source.id
  start_ip_address = "0.0.0.0"
  end_ip_address   = "0.0.0.0"
}

# =============================================================================
# RBAC — Roles y Accesos (Fase 5)
# =============================================================================

# Data Factory necesita acceso al Data Lake
resource "azurerm_role_assignment" "adf_storage_contributor" {
  scope                = azurerm_storage_account.datalake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_data_factory.main.identity[0].principal_id
}

# Rol: Data Engineer (lectura/escritura en todas las capas)
resource "azurerm_role_assignment" "data_engineer_storage" {
  for_each             = toset(var.data_engineer_principal_ids)
  scope                = azurerm_storage_account.datalake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = each.value
}

# Rol: Analyst (solo lectura en Gold)
resource "azurerm_role_assignment" "analyst_gold_reader" {
  for_each             = toset(var.analyst_principal_ids)
  scope                = azurerm_storage_container.containers["gold"].resource_manager_id
  role_definition_name = "Storage Blob Data Reader"
  principal_id         = each.value
}
