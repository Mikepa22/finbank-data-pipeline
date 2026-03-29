# =============================================================================
# VARIABLES — Parametrización sin credenciales
# =============================================================================

variable "environment" {
  description = "Entorno de despliegue: dev, staging, prod"
  type        = string
  default     = "dev"
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "El entorno debe ser dev, staging o prod."
  }
}

variable "location" {
  description = "Región Azure para todos los recursos"
  type        = string
  default     = "eastus2"
}

variable "resource_prefix" {
  description = "Prefijo para el Resource Group"
  type        = string
  default     = "rg-finbank-pipeline"
}

variable "tags" {
  description = "Tags comunes para todos los recursos"
  type        = map(string)
  default = {
    project     = "finbank-pipeline"
    owner       = "data-engineering"
    managed_by  = "terraform"
  }
}

# --- Storage ---
variable "storage_account_name" {
  description = "Nombre base del Storage Account (ADLS Gen2)"
  type        = string
  default     = "stfinbankdatalake"
}

variable "storage_replication" {
  description = "Tipo de replicación del Storage Account"
  type        = string
  default     = "LRS"
}

variable "datalake_containers" {
  description = "Contenedores del Data Lake"
  type        = list(string)
  default     = ["bronze", "silver", "gold", "landing", "errors", "logs"]
}

# --- Key Vault ---
variable "key_vault_name" {
  description = "Nombre base del Key Vault"
  type        = string
  default     = "kv-finbank"
}

# --- Databricks ---
variable "databricks_workspace_name" {
  description = "Nombre base del workspace de Databricks"
  type        = string
  default     = "dbw-finbank"
}

variable "databricks_sku" {
  description = "SKU del workspace de Databricks"
  type        = string
  default     = "standard"
}

# --- Data Factory ---
variable "data_factory_name" {
  description = "Nombre base de Azure Data Factory"
  type        = string
  default     = "adf-finbank"
}

# --- Log Analytics ---
variable "log_analytics_name" {
  description = "Nombre base del workspace de Log Analytics"
  type        = string
  default     = "log-finbank"
}

variable "log_retention_days" {
  description = "Días de retención de logs"
  type        = number
  default     = 30
}

# --- SQL Database ---
variable "sql_server_name" {
  description = "Nombre base del SQL Server"
  type        = string
  default     = "sql-finbank-source"
}

variable "sql_database_name" {
  description = "Nombre de la base de datos transaccional"
  type        = string
  default     = "finbank_transactional"
}

variable "sql_sku" {
  description = "SKU de la base de datos SQL"
  type        = string
  default     = "Basic"
}

variable "sql_admin_login" {
  description = "Login del administrador SQL"
  type        = string
  default     = "finbank_admin"
}

variable "sql_admin_password" {
  description = "Password del administrador SQL (sensible)"
  type        = string
  sensitive   = true
}

# --- Alertas ---
variable "alert_email_recipients" {
  description = "Lista de emails para recibir alertas del pipeline"
  type        = list(string)
  default     = ["data-engineering@finbank.com"]
}

# --- RBAC Principal IDs ---
variable "data_engineer_principal_ids" {
  description = "Object IDs de los Data Engineers en Azure AD"
  type        = list(string)
  default     = []
}

variable "analyst_principal_ids" {
  description = "Object IDs de los Analistas en Azure AD"
  type        = list(string)
  default     = []
}
