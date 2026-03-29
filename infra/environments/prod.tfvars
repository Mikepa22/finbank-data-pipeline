# =============================================================================
# ENTORNO: prod
# Uso: terraform apply -var-file="environments/prod.tfvars"
# =============================================================================
environment          = "prod"
location             = "eastus2"
storage_replication  = "GRS"       # Geo-redundante en prod
databricks_sku       = "premium"   # Unity Catalog en prod
sql_sku              = "S1"        # Mayor capacidad en prod
log_retention_days   = 90

tags = {
  project     = "finbank-pipeline"
  owner       = "data-engineering"
  environment = "prod"
  managed_by  = "terraform"
}

alert_email_recipients = [
  "data-engineering@finbank.com",
  "platform-oncall@finbank.com"
]
# sql_admin_password se pasa via variable de entorno: TF_VAR_sql_admin_password
