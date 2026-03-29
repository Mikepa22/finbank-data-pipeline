# Databricks notebook source
# =============================================================================
# CAPA SILVER — Limpieza, conformación y enriquecimiento
# =============================================================================
# Responsabilidades:
#   - Eliminar duplicados exactos y registros con campos obligatorios nulos
#   - Estandarizar tipos de datos
#   - Validar integridad referencial (registros huérfanos → tabla errores)
#   - Enmascarar PII (SHA-256)
#   - Calcular ind_sospechoso (3σ en ventana 30 días por cliente)
#   - Generar reporte de calidad de datos
#   - Escribir en Delta Lake con esquema limpio
# =============================================================================

# COMMAND ----------

import hashlib
import time
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
from delta.tables import DeltaTable

# COMMAND ----------

# --- Configuración ---
STORAGE_ACCOUNT = dbutils.widgets.get("storage_account")
BATCH_ID = dbutils.widgets.get("batch_id")

BRONZE_PATH = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net"
SILVER_PATH = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net"
ERRORS_PATH = f"abfss://errors@{STORAGE_ACCOUNT}.dfs.core.windows.net"
LOGS_PATH = f"abfss://logs@{STORAGE_ACCOUNT}.dfs.core.windows.net"

PROCESSING_TS = datetime.utcnow()

# COMMAND ----------

# --- UDF para enmascaramiento SHA-256 ---
@F.udf(StringType())
def sha256_mask(value):
    if value is None:
        return None
    return hashlib.sha256(str(value).encode("utf-8")).hexdigest()

# COMMAND ----------

def read_bronze(table_name: str) -> DataFrame:
    """Lee una tabla Delta desde la capa Bronze."""
    return spark.read.format("delta").load(f"{BRONZE_PATH}/{table_name}")

def write_silver(df: DataFrame, table_name: str, partition_cols: list = None):
    """Escribe una tabla Delta en la capa Silver con MERGE para idempotencia."""
    path = f"{SILVER_PATH}/{table_name}"
    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.save(path)

def write_errors(df: DataFrame, table_name: str, error_type: str):
    """Envía registros rechazados a la tabla de errores."""
    if df.count() == 0:
        return
    (
        df.withColumn("_error_type", F.lit(error_type))
          .withColumn("_error_timestamp", F.lit(PROCESSING_TS).cast("timestamp"))
          .withColumn("_source_table", F.lit(table_name))
          .withColumn("_batch_id", F.lit(BATCH_ID))
          .write.format("delta").mode("append")
          .save(f"{ERRORS_PATH}/silver_rejected")
    )

# COMMAND ----------

class QualityReport:
    """Generador de reportes de calidad de datos."""

    def __init__(self):
        self.metrics = []

    def add(self, table: str, metric: str, value):
        self.metrics.append({
            "table": table,
            "metric": metric,
            "value": str(value),
            "timestamp": str(PROCESSING_TS),
            "batch_id": BATCH_ID,
        })

    def save(self):
        from pyspark.sql import Row
        if not self.metrics:
            return
        rows = [Row(**m) for m in self.metrics]
        df = spark.createDataFrame(rows)
        df.write.format("delta").mode("append").save(f"{LOGS_PATH}/silver_quality_report")
        print(f"📊 Reporte de calidad guardado ({len(self.metrics)} métricas)")

qr = QualityReport()

# COMMAND ----------

# =============================================================================
# PROCESAMIENTO POR TABLA
# =============================================================================

# --- 1. TB_CLIENTES_CORE → silver_clientes ---
def process_clientes():
    print("\n▶ Procesando TB_CLIENTES_CORE...")
    df = read_bronze("TB_CLIENTES_CORE")
    original_count = df.count()
    qr.add("TB_CLIENTES_CORE", "bronze_row_count", original_count)

    # Nulos por columna (para reporte)
    for col_name in df.columns:
        null_count = df.filter(F.col(col_name).isNull()).count()
        if null_count > 0:
            qr.add("TB_CLIENTES_CORE", f"null_pct_{col_name}", round(null_count / original_count, 4))

    # Eliminar duplicados exactos
    df = df.dropDuplicates()
    after_dedup = df.count()
    qr.add("TB_CLIENTES_CORE", "duplicates_removed", original_count - after_dedup)

    # Eliminar registros sin PK
    df_valid = df.filter(F.col("id_cli").isNotNull())
    df_rejected = df.filter(F.col("id_cli").isNull())
    write_errors(df_rejected, "TB_CLIENTES_CORE", "null_primary_key")
    qr.add("TB_CLIENTES_CORE", "rejected_null_pk", df_rejected.count())

    # Estandarizar tipos
    df_clean = (
        df_valid
        .withColumn("id_cli", F.col("id_cli").cast("int"))
        .withColumn("fec_nac", F.to_date("fec_nac"))
        .withColumn("fec_alta", F.to_date("fec_alta"))
        .withColumn("score_buro", F.col("score_buro").cast("int"))
        # Normalizar texto
        .withColumn("nomb_cli", F.upper(F.trim("nomb_cli")))
        .withColumn("apell_cli", F.upper(F.trim("apell_cli")))
        .withColumn("ciudad_res", F.upper(F.trim("ciudad_res")))
        .withColumn("depto_res", F.upper(F.trim("depto_res")))
        .withColumn("estado_cli", F.upper(F.trim("estado_cli")))
        # Enmascarar PII
        .withColumn("num_doc_hash", sha256_mask("num_doc"))
        .withColumn("nomb_cli_hash", sha256_mask("nomb_cli"))
        .withColumn("apell_cli_hash", sha256_mask("apell_cli"))
        # Eliminar columnas PII originales — solo hash disponible en Silver
        .drop("num_doc", "nomb_cli", "apell_cli")
        # Metadata
        .withColumn("_silver_timestamp", F.lit(PROCESSING_TS).cast("timestamp"))
    )

    write_silver(df_clean, "silver_clientes")
    qr.add("TB_CLIENTES_CORE", "silver_row_count", df_clean.count())
    qr.add("TB_CLIENTES_CORE", "conformity_rate", round(df_clean.count() / original_count, 4))
    print(f"  ✅ silver_clientes: {df_clean.count()} registros")
    return df_clean

# COMMAND ----------

# --- 2. TB_PRODUCTOS_CAT → silver_productos ---
def process_productos():
    print("\n▶ Procesando TB_PRODUCTOS_CAT...")
    df = read_bronze("TB_PRODUCTOS_CAT")
    original_count = df.count()

    df = df.dropDuplicates()
    df_clean = (
        df
        .withColumn("tasa_ea", F.col("tasa_ea").cast("double"))
        .withColumn("plazo_max_meses", F.col("plazo_max_meses").cast("int"))
        .withColumn("cuota_min", F.col("cuota_min").cast("double"))
        .withColumn("comision_admin", F.col("comision_admin").cast("double"))
        .withColumn("desc_prod", F.trim("desc_prod"))
        .withColumn("tip_prod", F.upper(F.trim("tip_prod")))
        .withColumn("estado_prod", F.upper(F.trim("estado_prod")))
        .withColumn("_silver_timestamp", F.lit(PROCESSING_TS).cast("timestamp"))
    )

    write_silver(df_clean, "silver_productos")
    qr.add("TB_PRODUCTOS_CAT", "silver_row_count", df_clean.count())
    print(f"  ✅ silver_productos: {df_clean.count()} registros")
    return df_clean

# COMMAND ----------

# --- 3. TB_SUCURSALES_RED → silver_sucursales ---
def process_sucursales():
    print("\n▶ Procesando TB_SUCURSALES_RED...")
    df = read_bronze("TB_SUCURSALES_RED")
    df = df.dropDuplicates()

    df_clean = (
        df
        .withColumn("latitud", F.col("latitud").cast("double"))
        .withColumn("longitud", F.col("longitud").cast("double"))
        .withColumn("activo", F.col("activo").cast("int"))
        .withColumn("ciudad", F.upper(F.trim("ciudad")))
        .withColumn("depto", F.upper(F.trim("depto")))
        .withColumn("tip_punto", F.upper(F.trim("tip_punto")))
        .withColumn("_silver_timestamp", F.lit(PROCESSING_TS).cast("timestamp"))
    )

    write_silver(df_clean, "silver_sucursales")
    qr.add("TB_SUCURSALES_RED", "silver_row_count", df_clean.count())
    print(f"  ✅ silver_sucursales: {df_clean.count()} registros")
    return df_clean

# COMMAND ----------

# --- 4. TB_MOV_FINANCIEROS → silver_movimientos ---
def process_movimientos(df_clientes):
    print("\n▶ Procesando TB_MOV_FINANCIEROS...")
    df = read_bronze("TB_MOV_FINANCIEROS")
    original_count = df.count()
    qr.add("TB_MOV_FINANCIEROS", "bronze_row_count", original_count)

    # Eliminar duplicados exactos (Anomalía 1: esperamos ~500)
    df_dedup = df.dropDuplicates(subset=[c for c in df.columns if not c.startswith("_")])
    dup_count = original_count - df_dedup.count()
    qr.add("TB_MOV_FINANCIEROS", "duplicates_removed", dup_count)
    print(f"  ⚠ {dup_count} duplicados exactos eliminados")

    # Eliminar fechas futuras (Anomalía 2)
    today = datetime.utcnow().strftime("%Y-%m-%d")
    df_valid_dates = df_dedup.filter(F.col("fec_mov") <= F.lit(today))
    df_future = df_dedup.filter(F.col("fec_mov") > F.lit(today))
    write_errors(df_future, "TB_MOV_FINANCIEROS", "future_date")
    qr.add("TB_MOV_FINANCIEROS", "rejected_future_dates", df_future.count())
    print(f"  ⚠ {df_future.count()} registros con fechas futuras rechazados")

    # Validar integridad referencial contra clientes
    valid_client_ids = df_clientes.select("id_cli").distinct()
    df_with_ref = df_valid_dates.join(
        valid_client_ids, on="id_cli", how="left"
    )
    # Los que no hacen match van a errores
    df_orphan = df_valid_dates.join(valid_client_ids, on="id_cli", how="left_anti")
    write_errors(df_orphan, "TB_MOV_FINANCIEROS", "referential_integrity_fail")
    qr.add("TB_MOV_FINANCIEROS", "rejected_orphan_records", df_orphan.count())

    df_ref_valid = df_valid_dates.join(valid_client_ids, on="id_cli", how="inner")

    # Estandarizar y enriquecer
    df_clean = (
        df_ref_valid
        .withColumn("id_mov", F.col("id_mov").cast("long"))
        .withColumn("id_cli", F.col("id_cli").cast("int"))
        .withColumn("fec_mov", F.to_date("fec_mov"))
        .withColumn("vr_mov", F.col("vr_mov").cast("double"))
        .withColumn("tip_mov", F.upper(F.trim("tip_mov")))
        .withColumn("cod_canal", F.upper(F.trim("cod_canal")))
        .withColumn("cod_estado_mov", F.upper(F.trim("cod_estado_mov")))
        # Enmascarar id_dispositivo (PII)
        .withColumn("id_dispositivo_hash", sha256_mask("id_dispositivo"))
        .drop("id_dispositivo")
        # Flag de horario hábil / no hábil
        .withColumn("hora_int", F.substring("hra_mov", 1, 2).cast("int"))
        .withColumn("flag_horario",
            F.when(
                (F.col("hora_int") >= 7) & (F.col("hora_int") <= 18),
                F.lit("HABIL")
            ).otherwise(F.lit("NO_HABIL"))
        )
        .drop("hora_int")
    )

    # --- REGLA DE NEGOCIO: ind_sospechoso (3σ rolling 30 días) ---
    # Promedio móvil de 30 días y desviación estándar por cliente
    window_30d = (
        Window
        .partitionBy("id_cli")
        .orderBy(F.col("fec_mov").cast("long"))
        .rangeBetween(-30 * 86400, 0)  # 30 días en segundos
    )

    df_with_stats = (
        df_clean
        .withColumn("avg_30d", F.avg("vr_mov").over(window_30d))
        .withColumn("std_30d", F.stddev("vr_mov").over(window_30d))
        .withColumn("ind_sospechoso",
            F.when(
                (F.col("std_30d").isNotNull()) &
                (F.col("std_30d") > 0) &
                (F.col("vr_mov") > F.col("avg_30d") + 3 * F.col("std_30d")),
                F.lit(1)
            ).otherwise(F.lit(0))
        )
    )

    suspicious_count = df_with_stats.filter(F.col("ind_sospechoso") == 1).count()
    qr.add("TB_MOV_FINANCIEROS", "suspicious_transactions_flagged", suspicious_count)

    df_final = df_with_stats.withColumn("_silver_timestamp", F.lit(PROCESSING_TS).cast("timestamp"))

    write_silver(df_final, "silver_movimientos", partition_cols=["fec_mov"])
    qr.add("TB_MOV_FINANCIEROS", "silver_row_count", df_final.count())
    qr.add("TB_MOV_FINANCIEROS", "conformity_rate", round(df_final.count() / original_count, 4))
    print(f"  ✅ silver_movimientos: {df_final.count()} registros ({suspicious_count} sospechosos)")
    return df_final

# COMMAND ----------

# --- 5. TB_OBLIGACIONES → silver_obligaciones ---
def process_obligaciones(df_clientes):
    print("\n▶ Procesando TB_OBLIGACIONES...")
    df = read_bronze("TB_OBLIGACIONES")
    original_count = df.count()

    df = df.dropDuplicates()

    # Rechazar registros con dias_mora negativo (Anomalía 3)
    df_valid = df.filter(F.col("dias_mora_act") >= 0)
    df_negative_mora = df.filter(F.col("dias_mora_act") < 0)
    write_errors(df_negative_mora, "TB_OBLIGACIONES", "negative_dias_mora")
    qr.add("TB_OBLIGACIONES", "rejected_negative_mora", df_negative_mora.count())

    # Rechazar sdo_capital > vr_aprobado (Anomalía 3)
    df_valid2 = df_valid.filter(F.col("sdo_capital") <= F.col("vr_aprobado"))
    df_inconsistent = df_valid.filter(F.col("sdo_capital") > F.col("vr_aprobado"))
    write_errors(df_inconsistent, "TB_OBLIGACIONES", "sdo_exceeds_aprobado")
    qr.add("TB_OBLIGACIONES", "rejected_inconsistent_sdo", df_inconsistent.count())

    # Validar integridad referencial
    valid_ids = df_clientes.select("id_cli").distinct()
    df_orphan = df_valid2.join(valid_ids, on="id_cli", how="left_anti")
    write_errors(df_orphan, "TB_OBLIGACIONES", "referential_integrity_fail")
    df_ref_valid = df_valid2.join(valid_ids, on="id_cli", how="inner")

    df_clean = (
        df_ref_valid
        .withColumn("id_oblig", F.col("id_oblig").cast("int"))
        .withColumn("id_cli", F.col("id_cli").cast("int"))
        .withColumn("vr_aprobado", F.col("vr_aprobado").cast("double"))
        .withColumn("vr_desembolsado", F.col("vr_desembolsado").cast("double"))
        .withColumn("sdo_capital", F.col("sdo_capital").cast("double"))
        .withColumn("vr_cuota", F.col("vr_cuota").cast("double"))
        .withColumn("dias_mora_act", F.col("dias_mora_act").cast("int"))
        .withColumn("num_cuotas_pend", F.col("num_cuotas_pend").cast("int"))
        .withColumn("fec_desembolso", F.to_date("fec_desembolso"))
        .withColumn("fec_venc", F.to_date("fec_venc"))
        # Enmascarar valores financieros sensibles
        .withColumn("sdo_capital_masked", F.round(F.col("sdo_capital") / 1000000, 2))
        .withColumn("vr_desembolsado_masked", F.round(F.col("vr_desembolsado") / 1000000, 2))
        .withColumn("_silver_timestamp", F.lit(PROCESSING_TS).cast("timestamp"))
    )

    write_silver(df_clean, "silver_obligaciones")
    qr.add("TB_OBLIGACIONES", "silver_row_count", df_clean.count())
    qr.add("TB_OBLIGACIONES", "conformity_rate", round(df_clean.count() / original_count, 4))
    print(f"  ✅ silver_obligaciones: {df_clean.count()} registros")
    return df_clean

# COMMAND ----------

# --- 6. TB_COMISIONES_LOG → silver_comisiones ---
def process_comisiones(df_clientes):
    print("\n▶ Procesando TB_COMISIONES_LOG...")
    df = read_bronze("TB_COMISIONES_LOG")
    original_count = df.count()

    df = df.dropDuplicates()

    valid_ids = df_clientes.select("id_cli").distinct()
    df_orphan = df.join(valid_ids, on="id_cli", how="left_anti")
    write_errors(df_orphan, "TB_COMISIONES_LOG", "referential_integrity_fail")
    df_ref_valid = df.join(valid_ids, on="id_cli", how="inner")

    df_clean = (
        df_ref_valid
        .withColumn("id_comision", F.col("id_comision").cast("int"))
        .withColumn("id_cli", F.col("id_cli").cast("int"))
        .withColumn("fec_cobro", F.to_date("fec_cobro"))
        .withColumn("vr_comision", F.col("vr_comision").cast("double"))
        .withColumn("tip_comision", F.upper(F.trim("tip_comision")))
        .withColumn("estado_cobro", F.upper(F.trim("estado_cobro")))
        .withColumn("_silver_timestamp", F.lit(PROCESSING_TS).cast("timestamp"))
    )

    write_silver(df_clean, "silver_comisiones")
    qr.add("TB_COMISIONES_LOG", "silver_row_count", df_clean.count())
    print(f"  ✅ silver_comisiones: {df_clean.count()} registros")
    return df_clean

# COMMAND ----------

# =============================================================================
# EJECUCIÓN PRINCIPAL
# =============================================================================
print("=" * 60)
print(f"SILVER PROCESSING — Batch: {BATCH_ID}")
print(f"Timestamp: {PROCESSING_TS}")
print("=" * 60)

df_clientes = process_clientes()
df_productos = process_productos()
df_sucursales = process_sucursales()
df_movimientos = process_movimientos(df_clientes)
df_obligaciones = process_obligaciones(df_clientes)
df_comisiones = process_comisiones(df_clientes)

# Guardar reporte de calidad
qr.save()

print("\n" + "=" * 60)
print("✅ SILVER PROCESSING COMPLETADO")
print("=" * 60)
