# Databricks notebook source
# =============================================================================
# CAPA BRONZE — Ingesta de datos crudos desde SQL Server a Delta Lake
# =============================================================================
# Responsabilidades:
#   - Copiar datos tal cual desde la fuente transaccional
#   - Agregar columnas de auditoría (_ingestion_timestamp, _source_system, _batch_id)
#   - Particionar por fecha de ingesta (año/mes/día)
#   - Soportar modo incremental (solo registros nuevos/modificados)
#   - Registrar log de ejecución
#   - Idempotente: re-ejecutable sin duplicados
# =============================================================================

# COMMAND ----------

import uuid
import time
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable

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

# --- Configuración ---
# Estos valores se inyectan desde el orquestador (ADF/Databricks Workflow)
# o se leen del Key Vault / widgets de Databricks

dbutils.widgets.text("storage_account", "stfinbankdatalakedev")
dbutils.widgets.text("sql_server", "sql-finbank-source-dev.database.windows.net")
dbutils.widgets.text("sql_database", "finbank_transactional")
dbutils.widgets.text("execution_mode", "full")  # full | incremental

STORAGE_ACCOUNT = dbutils.widgets.get("storage_account")
SQL_SERVER = dbutils.widgets.get("sql_server")
SQL_DATABASE = dbutils.widgets.get("sql_database")
EXECUTION_MODE = dbutils.widgets.get("execution_mode")

BRONZE_PATH = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net"
ERRORS_PATH = f"abfss://errors@{STORAGE_ACCOUNT}.dfs.core.windows.net"
LOGS_PATH = f"abfss://logs@{STORAGE_ACCOUNT}.dfs.core.windows.net"

BATCH_ID = str(uuid.uuid4())[:8]
INGESTION_TS = datetime.utcnow()
SOURCE_SYSTEM = "FINBANK_TRANSACTIONAL_SQL"

# COMMAND ----------

# --- Tablas fuente con sus PKs (para modo incremental) ---
SOURCE_TABLES = {
    "TB_CLIENTES_CORE": {"pk": "id_cli", "watermark_col": None},
    "TB_PRODUCTOS_CAT": {"pk": "cod_prod", "watermark_col": None},
    "TB_MOV_FINANCIEROS": {"pk": "id_mov", "watermark_col": "fec_mov"},
    "TB_OBLIGACIONES": {"pk": "id_oblig", "watermark_col": "fec_desembolso"},
    "TB_SUCURSALES_RED": {"pk": "cod_suc", "watermark_col": None},
    "TB_COMISIONES_LOG": {"pk": "id_comision", "watermark_col": "fec_cobro"},
}

# COMMAND ----------

def get_jdbc_url() -> str:
    """Construye la URL JDBC para SQL Server."""
    # La contraseña se obtiene del Key Vault via Databricks Secret Scope
    password = dbutils.secrets.get(scope="finbank-secrets", key="sql-admin-password")
    return (
        f"jdbc:sqlserver://{SQL_SERVER}:1433;"
        f"database={SQL_DATABASE};"
        f"encrypt=true;trustServerCertificate=false;"
        f"hostNameInCertificate=*.database.windows.net;"
        f"loginTimeout=30"
    )

def read_from_source(table_name: str) -> DataFrame:
    """Lee una tabla completa desde SQL Server."""
    jdbc_url = get_jdbc_url()
    password = dbutils.secrets.get(scope="finbank-secrets", key="sql-admin-password")

    return (
        spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", f"dbo.{table_name}")
        .option("user", "finbank_admin")
        .option("password", password)
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .load()
    )

# COMMAND ----------

def add_audit_columns(df: DataFrame) -> DataFrame:
    """Agrega columnas de auditoría requeridas por Bronze."""
    return (
        df
        .withColumn("_ingestion_timestamp", F.lit(INGESTION_TS).cast("timestamp"))
        .withColumn("_source_system", F.lit(SOURCE_SYSTEM))
        .withColumn("_batch_id", F.lit(BATCH_ID))
        # Columnas de partición derivadas
        .withColumn("ingestion_year", F.lit(INGESTION_TS.year))
        .withColumn("ingestion_month", F.lit(INGESTION_TS.month))
        .withColumn("ingestion_day", F.lit(INGESTION_TS.day))
    )

# COMMAND ----------

def get_last_watermark(table_name: str, watermark_col: str) -> str:
    """Obtiene el último watermark de una tabla Bronze para modo incremental."""
    bronze_table_path = f"{BRONZE_PATH}/{table_name}"
    try:
        if DeltaTable.isDeltaTable(spark, bronze_table_path):
            last_wm = (
                spark.read.format("delta").load(bronze_table_path)
                .agg(F.max(watermark_col).alias("max_wm"))
                .collect()[0]["max_wm"]
            )
            return str(last_wm) if last_wm else None
    except Exception:
        pass
    return None

# COMMAND ----------

def ingest_table(table_name: str, config: dict) -> dict:
    """
    Ingesta una tabla individual desde la fuente a Bronze.
    Retorna métricas de la ingesta.
    """
    start_time = time.time()
    metrics = {
        "table": table_name,
        "batch_id": BATCH_ID,
        "mode": EXECUTION_MODE,
        "status": "SUCCESS",
        "error": None,
    }

    try:
        # 1. Leer desde la fuente
        df_source = read_from_source(table_name)

        # 2. Modo incremental: filtrar por watermark
        if EXECUTION_MODE == "incremental" and config.get("watermark_col"):
            wm_col = config["watermark_col"]
            last_wm = get_last_watermark(table_name, wm_col)
            if last_wm:
                df_source = df_source.filter(F.col(wm_col) > F.lit(last_wm))
                print(f"  [INCREMENTAL] Filtrando {table_name} donde {wm_col} > {last_wm}")

        # 3. Agregar columnas de auditoría
        df_bronze = add_audit_columns(df_source)

        # 4. Escribir en Delta Lake con particionamiento
        bronze_table_path = f"{BRONZE_PATH}/{table_name}"

        if EXECUTION_MODE == "full":
            (
                df_bronze.write
                .format("delta")
                .mode("overwrite")
                .partitionBy("ingestion_year", "ingestion_month", "ingestion_day")
                .option("overwriteSchema", "true")
                .save(bronze_table_path)
            )
        else:
            # Modo incremental: MERGE para idempotencia
            pk = config["pk"]
            if DeltaTable.isDeltaTable(spark, bronze_table_path):
                delta_table = DeltaTable.forPath(spark, bronze_table_path)
                (
                    delta_table.alias("target")
                    .merge(
                        df_bronze.alias("source"),
                        f"target.{pk} = source.{pk}"
                    )
                    .whenMatchedUpdateAll()
                    .whenNotMatchedInsertAll()
                    .execute()
                )
            else:
                (
                    df_bronze.write
                    .format("delta")
                    .mode("overwrite")
                    .partitionBy("ingestion_year", "ingestion_month", "ingestion_day")
                    .save(bronze_table_path)
                )

        # 5. Métricas
        row_count = df_bronze.count()
        duration = round(time.time() - start_time, 2)
        metrics["row_count"] = row_count
        metrics["duration_seconds"] = duration
        metrics["output_path"] = bronze_table_path

        print(f"  ✅ {table_name}: {row_count} registros → Bronze ({duration}s)")

    except Exception as e:
        metrics["status"] = "FAILED"
        metrics["error"] = str(e)
        metrics["duration_seconds"] = round(time.time() - start_time, 2)
        print(f"  ❌ {table_name}: ERROR — {e}")

    return metrics

# COMMAND ----------

# --- Guardar log de ingesta ---
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

schema = StructType([
    StructField("table", StringType()),
    StructField("row_count", LongType()),
    StructField("duration_seconds", DoubleType()),
    StructField("status", StringType()),
    StructField("batch_id", StringType()),
])

# Normalizar métricas
log_data = [(m["table"], int(m.get("row_count", 0)), float(m.get("duration_seconds", 0)), 
             m.get("status", "UNKNOWN"), m.get("batch_id", "")) for m in all_metrics]

df_log = spark.createDataFrame(log_data, schema)
df_log.write.format("delta").mode("append").save(f"{LOGS_PATH}/bronze_ingestion_log")

print(f"\nLog guardado ({len(all_metrics)} tablas)")

# Resumen
total_rows = sum(m.get("row_count", 0) for m in all_metrics)
print(f"Bronze completado: {total_rows:,} registros en {len(all_metrics)} tablas")

# COMMAND ----------

# =============================================================================
# EJECUCIÓN PRINCIPAL
# =============================================================================
print("=" * 60)
print(f"BRONZE INGESTION — Batch: {BATCH_ID}")
print(f"Modo: {EXECUTION_MODE} | Timestamp: {INGESTION_TS}")
print("=" * 60)

all_metrics = []
for table_name, config in SOURCE_TABLES.items():
    metrics = ingest_table(table_name, config)
    all_metrics.append(metrics)

# Escribir log
#write_ingestion_log(all_metrics)

# Validar resultados
failed = [m for m in all_metrics if m["status"] == "FAILED"]
if failed:
    failed_tables = ", ".join([m["table"] for m in failed])
    raise Exception(f"Bronze ingestion failed for: {failed_tables}")

total_rows = sum(m.get("row_count", 0) for m in all_metrics)
print(f"\n✅ Bronze completado: {total_rows} registros totales en {len(all_metrics)} tablas")

# COMMAND ----------

# Pasar métricas al siguiente paso del pipeline via task values (Databricks Workflows)
# dbutils.jobs.taskValues.set(key="bronze_total_rows", value=total_rows)
# dbutils.jobs.taskValues.set(key="bronze_batch_id", value=BATCH_ID)
