# =============================================================================
# ENTORNO: dev
# Uso: terraform apply -var-file="environments/dev.tfvars"
# =============================================================================
environment          = "dev"
location             = "eastus2"
storage_replication  = "LRS"
databricks_sku       = "standard"
sql_sku              = "Basic"
log_retention_days   = 30

tags = {
  project     = "finbank-pipeline"
  owner       = "data-engineering"
  environment = "dev"
  managed_by  = "terraform"
}

alert_email_recipients = ["dev-alerts@finbank.com"]
# sql_admin_password se pasa via variable de entorno: TF_VAR_sql_admin_password
