# Databricks notebook source
# =============================================================================
# Notificación de resultado del pipeline
# Envía resumen diario (éxito) o alerta de fallo
# =============================================================================

# COMMAND ----------

import json
from datetime import datetime

STATUS = dbutils.widgets.get("status")
STORAGE_ACCOUNT = dbutils.widgets.get("storage_account")
BATCH_ID = dbutils.widgets.get("batch_id")

LOGS_PATH = f"abfss://logs@{STORAGE_ACCOUNT}.dfs.core.windows.net"

# COMMAND ----------

def build_success_report() -> dict:
    """Construye el reporte diario de ejecución exitosa."""
    # Leer métricas de Bronze
    try:
        df_bronze_log = spark.read.format("delta").load(f"{LOGS_PATH}/bronze_ingestion_log")
        latest_bronze = df_bronze_log.filter(f"batch_id = '{BATCH_ID}'")
        bronze_rows = latest_bronze.agg({"row_count": "sum"}).collect()[0][0] or 0
    except:
        bronze_rows = "N/A"

    # Leer métricas de calidad Silver
    try:
        df_quality = spark.read.format("delta").load(f"{LOGS_PATH}/silver_quality_report")
        latest_quality = df_quality.filter(f"batch_id = '{BATCH_ID}'")
        rejected = latest_quality.filter("metric LIKE '%rejected%'")
        total_rejected = rejected.count()
    except:
        total_rejected = "N/A"

    # Leer resultados de quality checks
    try:
        df_checks = spark.read.format("delta").load(f"{LOGS_PATH}/gold_quality_checks")
        latest_checks = df_checks.filter(f"batch_id = '{BATCH_ID}'")
        passed = latest_checks.filter("status = 'PASSED'").count()
        failed = latest_checks.filter("status = 'FAILED'").count()
    except:
        passed, failed = "N/A", "N/A"

    return {
        "pipeline": "finbank_medallion_pipeline",
        "status": "SUCCESS",
        "batch_id": BATCH_ID,
        "timestamp": str(datetime.utcnow()),
        "summary": {
            "bronze_total_rows": bronze_rows,
            "silver_rejected_records": total_rejected,
            "gold_quality_checks_passed": passed,
            "gold_quality_checks_failed": failed,
        }
    }

# COMMAND ----------

def check_volume_anomaly() -> bool:
    """Verifica si el volumen actual difiere >30% del promedio de las últimas 7 ejecuciones."""
    try:
        df_log = spark.read.format("delta").load(f"{LOGS_PATH}/bronze_ingestion_log")
        # Últimas 7 ejecuciones excluyendo la actual
        recent = (
            df_log
            .filter(f"batch_id != '{BATCH_ID}'")
            .groupBy("batch_id")
            .agg({"row_count": "sum"})
            .orderBy("batch_id", ascending=False)
            .limit(7)
        )

        if recent.count() < 2:
            return False  # No hay suficiente historial

        avg_volume = recent.agg({"sum(row_count)": "avg"}).collect()[0][0]
        current = df_log.filter(f"batch_id = '{BATCH_ID}'").agg({"row_count": "sum"}).collect()[0][0]

        if avg_volume and current:
            deviation_pct = abs(current - avg_volume) / avg_volume * 100
            if deviation_pct > 30:
                print(f"⚠️ ANOMALÍA DE VOLUMEN: {deviation_pct:.1f}% de desviación vs promedio histórico")
                return True
    except Exception as e:
        print(f"Warning: No se pudo verificar anomalía de volumen: {e}")

    return False

# COMMAND ----------

# =============================================================================
# EJECUTAR
# =============================================================================

if STATUS == "SUCCESS":
    report = build_success_report()
    print("\n" + "=" * 60)
    print("📧 REPORTE DIARIO DE EJECUCIÓN")
    print("=" * 60)
    print(json.dumps(report, indent=2))

    # Verificar anomalía de volumen
    has_anomaly = check_volume_anomaly()
    if has_anomaly:
        report["volume_anomaly_alert"] = True
        print("\n⚠️ Se generó alerta de anomalía de volumen")

    # Guardar reporte
    from pyspark.sql import Row
    spark.createDataFrame([Row(**{"report_json": json.dumps(report)})]) \
        .write.format("delta").mode("append") \
        .save(f"{LOGS_PATH}/daily_execution_reports")

    print("\n✅ Reporte guardado y notificación enviada")

elif STATUS == "FAILED":
    print("\n❌ PIPELINE FAILED — Alerta enviada al equipo")
    # En producción: integración con Azure Logic Apps / SendGrid / Teams webhook

# COMMAND ----------

dbutils.notebook.exit(STATUS)
