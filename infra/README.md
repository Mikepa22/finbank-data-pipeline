# Infraestructura — Instrucciones de Despliegue

## Pre-requisitos

1. **Azure CLI** autenticado: `az login`
2. **Terraform** >= 1.5.0: `terraform -version`
3. **Subscription** con permisos de Owner o Contributor
4. **Storage Account para Terraform state** (crear una sola vez):

```bash
# Crear resource group y storage para el state de Terraform
az group create --name rg-terraform-state --location eastus2
az storage account create \
  --name stterraformfinbank \
  --resource-group rg-terraform-state \
  --sku Standard_LRS \
  --kind StorageV2
az storage container create \
  --name tfstate \
  --account-name stterraformfinbank
```

## Despliegue

### 1. Inicializar Terraform
```bash
cd infra/
terraform init
```

### 2. Planificar (verificar cambios)
```bash
# Dev
export TF_VAR_sql_admin_password="<PASSWORD_SEGURO>"
terraform plan -var-file="environments/dev.tfvars"

# Prod
terraform plan -var-file="environments/prod.tfvars"
```

### 3. Aplicar
```bash
terraform apply -var-file="environments/dev.tfvars"
```

### 4. Verificar outputs
```bash
terraform output
```

## Recursos Creados

| Recurso | Nombre (dev) | Región | Propósito |
|---------|-------------|--------|-----------|
| Resource Group | rg-finbank-pipeline-dev | eastus2 | Contenedor de todos los recursos |
| Storage Account (ADLS Gen2) | stfinbankdatalakedev | eastus2 | Data Lake con contenedores bronze/silver/gold/landing/errors/logs |
| Databricks Workspace | dbw-finbank-dev | eastus2 | Motor de procesamiento Spark + Delta Lake |
| Azure Data Factory | adf-finbank-dev | eastus2 | Orquestación y linked services |
| Key Vault | kv-finbank-dev | eastus2 | Gestión de secretos (contraseñas, connection strings) |
| Log Analytics | log-finbank-dev | eastus2 | Monitoreo y auditoría de accesos |
| SQL Server + Database | sql-finbank-source-dev | eastus2 | Base de datos transaccional (fuente) |
| Action Group | ag-finbank-pipeline-dev | eastus2 | Alertas por email ante fallos del pipeline |

## Destruir (solo para dev/pruebas)
```bash
terraform destroy -var-file="environments/dev.tfvars"
```

## Notas de Seguridad

- **NUNCA** commitear `terraform.tfstate` al repositorio (ya está en `.gitignore`)
- **NUNCA** poner passwords en archivos `.tfvars` — usar `TF_VAR_sql_admin_password`
- El backend remoto tiene locking automático para evitar conflictos
- Key Vault usa RBAC authorization (no access policies legacy)
