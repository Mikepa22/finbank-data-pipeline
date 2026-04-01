# =============================================================================
# ENTORNO: dev
# Uso: terraform apply -var-file="environments/dev.tfvars"
# =============================================================================
environment          = "dev"
location             = "westus2"
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

alert_email_recipients = ["Mikedataeng@outlook.com"]
# sql_admin_password se pasa via variable de entorno: TF_VAR_sql_admin_password
