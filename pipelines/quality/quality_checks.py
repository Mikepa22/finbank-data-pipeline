# Databricks notebook source
# =============================================================================
# VERIFICACIONES DE CALIDAD DE DATOS — Capa Gold
# 5 checks automatizados (equivalente a Great Expectations)
# =============================================================================

# COMMAND ----------

from datetime import datetime
from pyspark.sql import functions as F

STORAGE_ACCOUNT = dbutils.widgets.get("storage_account")
GOLD_PATH = f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net"
LOGS_PATH = f"abfss://logs@{STORAGE_ACCOUNT}.dfs.core.windows.net"
BATCH_ID = dbutils.widgets.get("batch_id")

results = []

def check(name: str, table: str, passed: bool, details: str):
    status = "PASSED" if passed else "FAILED"
    icon = "✅" if passed else "❌"
    results.append({
        "check_name": name,
        "table": table,
        "status": status,
        "details": details,
        "timestamp": str(datetime.utcnow()),
        "batch_id": BATCH_ID,
    })
    print(f"  {icon} [{status}] {name} — {table}: {details}")

# COMMAND ----------

# --- CHECK 1: Row count > 0 en todas las tablas Gold ---
print("\n▶ CHECK 1: Volumen (row_count_not_zero)")
gold_tables = [
    "dim_clientes", "dim_productos", "dim_geografia", "dim_canal",
    "fact_transacciones", "fact_cartera", "fact_rentabilidad_cliente",
    "kpi_cartera_diaria",
]
for t in gold_tables:
    try:
        count = spark.read.format("delta").load(f"{GOLD_PATH}/{t}").count()
        check("row_count_not_zero", t, count > 0, f"{count} registros")
    except Exception as e:
        check("row_count_not_zero", t, False, f"Error: {e}")

# COMMAND ----------

# --- CHECK 2: No hay PKs nulas en dimensiones ---
print("\n▶ CHECK 2: Completeness (no_null_primary_keys)")
pk_map = {
    "dim_clientes": "id_cli",
    "dim_productos": "producto_id",
    "dim_canal": "cod_suc",
    "fact_transacciones": "id_mov",
    "fact_cartera": "id_oblig",
}
for table, pk in pk_map.items():
    df = spark.read.format("delta").load(f"{GOLD_PATH}/{table}")
    null_count = df.filter(F.col(pk).isNull()).count()
    check("no_null_primary_keys", table, null_count == 0,
          f"{null_count} PKs nulas en {pk}")

# COMMAND ----------

# --- CHECK 3: Integridad referencial fact → dim ---
print("\n▶ CHECK 3: Consistency (referential_integrity)")
dim_clientes_ids = (
    spark.read.format("delta").load(f"{GOLD_PATH}/dim_clientes")
    .select("id_cli").distinct()
)

for fact_table in ["fact_transacciones", "fact_cartera"]:
    df_fact = spark.read.format("delta").load(f"{GOLD_PATH}/{fact_table}")
    orphans = df_fact.join(dim_clientes_ids, on="id_cli", how="left_anti").count()
    check("referential_integrity", fact_table, orphans == 0,
          f"{orphans} registros huérfanos sin dim_clientes")

# COMMAND ----------

# --- CHECK 4: Rangos válidos de fechas ---
print("\n▶ CHECK 4: Validity (valid_date_ranges)")
df_trans = spark.read.format("delta").load(f"{GOLD_PATH}/fact_transacciones")
today = datetime.utcnow().strftime("%Y-%m-%d")

future_count = df_trans.filter(F.col("fec_mov") > F.lit(today)).count()
check("valid_date_ranges", "fact_transacciones", future_count == 0,
      f"{future_count} transacciones con fecha futura")

old_count = df_trans.filter(F.col("fec_mov") < F.lit("2020-01-01")).count()
check("valid_date_ranges_min", "fact_transacciones", old_count == 0,
      f"{old_count} transacciones anteriores a 2020")

# COMMAND ----------

# --- CHECK 5: No hay duplicados exactos en fact tables ---
print("\n▶ CHECK 5: Uniqueness (no_exact_duplicates)")
for fact_table, pk in [("fact_transacciones", "id_mov"), ("fact_cartera", "id_oblig")]:
    df = spark.read.format("delta").load(f"{GOLD_PATH}/{fact_table}")
    total = df.count()
    distinct = df.select(pk).distinct().count()
    dups = total - distinct
    check("no_exact_duplicates", fact_table, dups == 0,
          f"{dups} PKs duplicadas en {pk} (total: {total}, distinct: {distinct})")

# COMMAND ----------

# --- Guardar resultados ---
from pyspark.sql import Row
df_results = spark.createDataFrame([Row(**r) for r in results])
(
    df_results.write.format("delta").mode("append")
    .save(f"{LOGS_PATH}/gold_quality_checks")
)

# Resumen
total = len(results)
passed = sum(1 for r in results if r["status"] == "PASSED")
failed = sum(1 for r in results if r["status"] == "FAILED")

print(f"\n{'=' * 60}")
print(f"QUALITY CHECK SUMMARY: {passed}/{total} passed, {failed} failed")
print(f"{'=' * 60}")

if failed > 0:
    failed_checks = [r for r in results if r["status"] == "FAILED"]
    for fc in failed_checks:
        print(f"  ❌ {fc['check_name']} — {fc['table']}: {fc['details']}")
    # No lanzar excepción; la alerta se maneja por el orquestador
    dbutils.notebook.exit(f"QUALITY_WARNINGS:{failed}")
else:
    dbutils.notebook.exit("ALL_CHECKS_PASSED")
