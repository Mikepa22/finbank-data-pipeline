# Databricks notebook source
# =============================================================================
# SETUP INICIAL — Configuración de Databricks Workspace
# =============================================================================
# Ejecutar UNA VEZ después del terraform apply.
# Configura:
#   1. Secret Scope vinculado a Azure Key Vault
#   2. Mount points para ADLS Gen2 (si no se usa abfss:// directo)
#   3. Creación de base de datos en Unity Catalog / Hive metastore
#   4. Verificación de conectividad
# =============================================================================

# COMMAND ----------

# --- 1. Verificar Secret Scope ---
# El Secret Scope se crea via Databricks CLI (no via notebook):
#
# databricks secrets create-scope \
#   --scope finbank-secrets \
#   --scope-backend-type AZURE_KEYVAULT \
#   --resource-id /subscriptions/<SUB_ID>/resourceGroups/rg-finbank-pipeline-dev/providers/Microsoft.KeyVault/vaults/kv-finbank-dev \
#   --dns-name https://kv-finbank-dev.vault.azure.net/
#
# Secretos requeridos en Key Vault:
#   - sql-admin-password: contraseña del SQL Server
#   - storage-account-key: (opcional, si no se usa Managed Identity)

try:
    scopes = dbutils.secrets.listScopes()
    scope_names = [s.name for s in scopes]
    if "finbank-secrets" in scope_names:
        print("✅ Secret Scope 'finbank-secrets' encontrado")
        secrets = dbutils.secrets.list(scope="finbank-secrets")
        print(f"   Secretos disponibles: {[s.key for s in secrets]}")
    else:
        print("❌ Secret Scope 'finbank-secrets' NO encontrado")
        print("   Crear con: databricks secrets create-scope --scope finbank-secrets ...")
except Exception as e:
    print(f"⚠️ Error verificando scopes: {e}")

# COMMAND ----------

# --- 2. Configurar acceso a ADLS Gen2 via Service Principal ---
# En producción se usa Managed Identity (Unity Catalog).
# Para dev, configurar via Spark conf con Service Principal:

# STORAGE_ACCOUNT = "stfinbankdatalakedev"
# APP_ID = dbutils.secrets.get(scope="finbank-secrets", key="sp-app-id")
# SP_SECRET = dbutils.secrets.get(scope="finbank-secrets", key="sp-secret")
# TENANT_ID = dbutils.secrets.get(scope="finbank-secrets", key="tenant-id")

# spark.conf.set(f"fs.azure.account.auth.type.{STORAGE_ACCOUNT}.dfs.core.windows.net", "OAuth")
# spark.conf.set(f"fs.azure.account.oauth.provider.type.{STORAGE_ACCOUNT}.dfs.core.windows.net",
#                "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
# spark.conf.set(f"fs.azure.account.oauth2.client.id.{STORAGE_ACCOUNT}.dfs.core.windows.net", APP_ID)
# spark.conf.set(f"fs.azure.account.oauth2.client.secret.{STORAGE_ACCOUNT}.dfs.core.windows.net", SP_SECRET)
# spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{STORAGE_ACCOUNT}.dfs.core.windows.net",
#                f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/token")

print("ℹ️ Configuración de acceso a ADLS Gen2 — usar Managed Identity en prod")

# COMMAND ----------

# --- Configurar acceso a ADLS Gen2 ---
STORAGE_ACCOUNT = "stfinbankdatalakedev"

# Obtener la key del Storage Account desde Key Vault
storage_key = dbutils.secrets.get(scope="finbank-secrets", key="storage-account-key")

spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    storage_key
)
print("✅ Acceso a ADLS Gen2 configurado")

# COMMAND ----------

# --- 3. Verificar acceso a los contenedores del Data Lake ---
STORAGE_ACCOUNT = "stfinbankdatalakedev"

containers = ["bronze", "silver", "gold", "errors", "logs"]
for c in containers:
    path = f"abfss://{c}@{STORAGE_ACCOUNT}.dfs.core.windows.net/"
    try:
        dbutils.fs.ls(path)
        print(f"  ✅ Container '{c}' accesible")
    except Exception as e:
        if "FilesystemNotFound" in str(e):
            print(f"  ✅ Container '{c}' existe (vacío)")
        else:
            print(f"  ❌ Container '{c}' error: {e}")

print("\n✅ Acceso al Data Lake verificado — listo para ejecutar el pipeline")

# COMMAND ----------

# --- 4. Verificar conectividad a SQL Server ---
STORAGE_ACCOUNT = "stfinbankdatalakedev"

print("\n🔍 Verificando conectividad...")

# Test ADLS Gen2
try:
    containers = ["bronze", "silver", "gold", "errors", "logs", "landing"]
    for c in containers:
        path = f"abfss://{c}@{STORAGE_ACCOUNT}.dfs.core.windows.net/"
        try:
            dbutils.fs.ls(path)
            print(f"  ✅ Container '{c}' accesible")
        except Exception:
            print(f"  ⚠️ Container '{c}' vacío o sin acceso (normal si es primera ejecución)")
except Exception as e:
    print(f"  ❌ Error accediendo ADLS: {e}")

# Test SQL Server
try:
    sql_password = dbutils.secrets.get(scope="finbank-secrets", key="sql-admin-password")
    jdbc_url = (
        "jdbc:sqlserver://sql-finbank-source-dev.database.windows.net:1433;"
        "database=finbank_transactional;encrypt=true;trustServerCertificate=false;"
        "hostNameInCertificate=*.database.windows.net;loginTimeout=30"
    )
    df_test = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("query", "SELECT 1 AS test")
        .option("user", "finbank_admin")
        .option("password", sql_password)
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .load()
    )
    print(f"  ✅ SQL Server conectado exitosamente")
except Exception as e:
    print(f"  ❌ SQL Server no accesible: {e}")

# COMMAND ----------

# --- 5. Registrar tablas Delta existentes (si ya hay datos en Gold) ---
print("\n📋 Registrando tablas Delta en metastore...")

gold_tables = [
    "dim_clientes", "dim_productos", "dim_geografia", "dim_canal",
    "fact_transacciones", "fact_cartera", "fact_rentabilidad_cliente",
    "kpi_cartera_diaria"
]

for table in gold_tables:
    path = f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net/{table}"
    try:
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS finbank_gold.{table}
            USING DELTA
            LOCATION '{path}'
        """)
        count = spark.sql(f"SELECT COUNT(*) FROM finbank_gold.{table}").collect()[0][0]
        print(f"  ✅ finbank_gold.{table}: {count:,} registros")
    except Exception:
        print(f"  ⚠️ finbank_gold.{table}: aún no existe (se creará con el pipeline)")

# COMMAND ----------

print("\n" + "=" * 60)
print("SETUP COMPLETADO")
print("=" * 60)
print("Próximo paso: ejecutar el pipeline via Databricks Workflow")
print("  databricks jobs create --json @orchestration/workflow_definition.json")
print("  databricks jobs run-now --job-id <JOB_ID>")
