# Databricks notebook source
# =============================================================================
# CAPA GOLD — Modelo dimensional y reglas de negocio
# Escenario A: Banca y Servicios Financieros — FinBank S.A.
# =============================================================================
# Tablas destino:
#   dim_clientes, dim_productos, dim_geografia, dim_canal, dim_fecha
#   fact_transacciones, fact_cartera, fact_rentabilidad_cliente
#   kpi_cartera_diaria (tabla de KPIs ejecutivos)
# =============================================================================

# COMMAND ----------

import time
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *

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

dbutils.widgets.text("storage_account", "stfinbankdatalakedev")
dbutils.widgets.text("batch_id", "bf48b24e")

STORAGE_ACCOUNT = dbutils.widgets.get("storage_account")
BATCH_ID = dbutils.widgets.get("batch_id")

SILVER_PATH = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net"
GOLD_PATH = f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net"
PROCESSING_TS = datetime.utcnow()

# --- Parámetros de negocio (desde config parametrizado) ---
FX_COP_USD = 0.000245
SEGMENTOS_MAP = {"01": "Básico", "02": "Estándar", "03": "Premium", "04": "Elite"}

MORA_BUCKETS = [
    (0, 0, "Al día"),
    (1, 30, "Rango 1"),
    (31, 60, "Rango 2"),
    (61, 90, "Rango 3"),
    (91, 999999, "Deteriorado"),
]

REGULATORY = [
    ("A", 0, 0, 0.01),
    ("B", 1, 30, 0.032),
    ("C", 31, 60, 0.117),
    ("D", 61, 90, 0.50),
    ("E", 91, 999999, 1.00),
]

# COMMAND ----------

def read_silver(table: str) -> DataFrame:
    return spark.read.format("delta").load(f"{SILVER_PATH}/{table}")

def write_gold(df: DataFrame, table: str, partition_cols: list = None):
    path = f"{GOLD_PATH}/{table}"
    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.save(path)
    count = df.count()
    print(f"  ✅ {table}: {count} registros → Gold")
    return count

# COMMAND ----------

# =============================================================================
# DIMENSIONES
# =============================================================================

# --- dim_clientes ---
def build_dim_clientes():
    print("\n▶ Construyendo dim_clientes...")
    df = read_silver("silver_clientes")

    # Segmento mapping
    segmento_expr = F.create_map([F.lit(x) for kv in SEGMENTOS_MAP.items() for x in kv])

    dim = (
        df
        .withColumn("nombre_completo_hash",
            F.concat(F.col("nomb_cli_hash"), F.lit("|"), F.col("apell_cli_hash")))
        .withColumn("edad",
            F.floor(F.datediff(F.current_date(), F.col("fec_nac")) / 365.25).cast("int"))
        .withColumn("grupo_edad",
            F.when(F.col("edad") < 25, "18-24")
            .when(F.col("edad") < 35, "25-34")
            .when(F.col("edad") < 45, "35-44")
            .when(F.col("edad") < 55, "45-54")
            .when(F.col("edad") < 65, "55-64")
            .otherwise("65+"))
        .withColumn("segmento_desc", segmento_expr[F.col("cod_segmento")])
        .withColumn("antiguedad_dias",
            F.datediff(F.current_date(), F.col("fec_alta")).cast("int"))
        .select(
            "id_cli",
            "nombre_completo_hash",
            "num_doc_hash",
            "tip_doc",
            "fec_nac",
            "edad",
            "grupo_edad",
            "fec_alta",
            "antiguedad_dias",
            "cod_segmento",
            "segmento_desc",
            "score_buro",
            "ciudad_res",
            "depto_res",
            "estado_cli",
            "canal_adquis",
        )
        .withColumn("_gold_timestamp", F.lit(PROCESSING_TS).cast("timestamp"))
    )
    return write_gold(dim, "dim_clientes")

# COMMAND ----------

# --- dim_productos ---
def build_dim_productos():
    print("\n▶ Construyendo dim_productos...")
    df = read_silver("silver_productos")

    dim = (
        df
        .withColumnRenamed("cod_prod", "producto_id")
        .withColumnRenamed("desc_prod", "producto_nombre")
        .withColumnRenamed("tip_prod", "tipo_producto")
        .withColumnRenamed("tasa_ea", "tasa_efectiva_anual")
        .withColumnRenamed("plazo_max_meses", "plazo_maximo_meses")
        .withColumnRenamed("cuota_min", "cuota_minima")
        .withColumnRenamed("comision_admin", "comision_administracion")
        .withColumnRenamed("estado_prod", "estado_producto")
        # Tasa mensual equivalente: (1 + tasa_ea)^(1/12) - 1
        .withColumn("tasa_mensual_equivalente",
            F.round(F.pow(1 + F.col("tasa_efectiva_anual"), 1.0/12) - 1, 6))
        # Clasificar en familia
        .withColumn("familia_producto",
            F.when(F.col("tipo_producto").isin(
                "CREDITO_LIBRE_INVERSION", "CREDITO_ROTATIVO", "TARJETA_DIGITAL"),
                "Crédito")
            .when(F.col("tipo_producto") == "CUENTA_AHORRO", "Ahorro")
            .otherwise("Transaccional"))
        .withColumn("_gold_timestamp", F.lit(PROCESSING_TS).cast("timestamp"))
    )
    return write_gold(dim, "dim_productos")

# COMMAND ----------

# --- dim_geografia y dim_canal (desde TB_SUCURSALES_RED) ---
def build_dim_geografia_canal():
    print("\n▶ Construyendo dim_geografia y dim_canal...")
    df = read_silver("silver_sucursales")

    # dim_geografia: ciudad + departamento
    dim_geo = (
        df.select("ciudad", "depto")
        .distinct()
        .withColumn("geo_id", F.monotonically_increasing_id() + 1)
        .withColumn("_gold_timestamp", F.lit(PROCESSING_TS).cast("timestamp"))
    )
    write_gold(dim_geo, "dim_geografia")

    # dim_canal: tipo de punto de atención y canal digital
    dim_canal = (
        df.select("cod_suc", "nom_suc", "tip_punto", "ciudad", "depto",
                   "latitud", "longitud", "activo")
        .withColumn("canal_tipo",
            F.when(F.col("tip_punto") == "DIGITAL", "Digital")
            .when(F.col("tip_punto") == "CORRESPONSAL", "Corresponsal")
            .when(F.col("tip_punto") == "OFICINA", "Oficina")
            .otherwise("Otro"))
        .withColumn("_gold_timestamp", F.lit(PROCESSING_TS).cast("timestamp"))
    )
    return write_gold(dim_canal, "dim_canal")

# COMMAND ----------

# =============================================================================
# TABLAS DE HECHOS
# =============================================================================

# --- fact_transacciones ---
def build_fact_transacciones():
    print("\n▶ Construyendo fact_transacciones...")
    df = read_silver("silver_movimientos")

    fact = (
        df
        # Calcular monto en USD
        .withColumn("vr_mov_usd", F.round(F.col("vr_mov") * FX_COP_USD, 2))
        # Extraer componentes de fecha para partición
        .withColumn("fec_mov_year", F.year("fec_mov"))
        .withColumn("fec_mov_month", F.month("fec_mov"))
        .select(
            "id_mov", "id_cli", "cod_prod", "num_cuenta",
            "fec_mov", "hra_mov", "vr_mov", "vr_mov_usd",
            "tip_mov", "cod_canal", "cod_ciudad", "cod_estado_mov",
            "flag_horario", "ind_sospechoso",
            "avg_30d", "std_30d",
            "fec_mov_year", "fec_mov_month",
        )
        .withColumn("_gold_timestamp", F.lit(PROCESSING_TS).cast("timestamp"))
    )

    return write_gold(fact, "fact_transacciones",
                      partition_cols=["fec_mov_year", "fec_mov_month"])

# COMMAND ----------

# --- fact_cartera (con bucket_mora, clasificación regulatoria, provisión) ---
def build_fact_cartera():
    print("\n▶ Construyendo fact_cartera...")
    df = read_silver("silver_obligaciones")

    # bucket_mora
    bucket_expr = F.lit("Desconocido")
    for min_d, max_d, label in MORA_BUCKETS:
        bucket_expr = F.when(
            (F.col("dias_mora_act") >= min_d) & (F.col("dias_mora_act") <= max_d),
            F.lit(label)
        ).otherwise(bucket_expr)

    # Clasificación regulatoria A/B/C/D/E
    calif_expr = F.lit("E")
    for rating, min_d, max_d, _ in REGULATORY:
        calif_expr = F.when(
            (F.col("dias_mora_act") >= min_d) & (F.col("dias_mora_act") <= max_d),
            F.lit(rating)
        ).otherwise(calif_expr)

    # Provisión estimada
    provision_expr = F.lit(1.0)
    for _, min_d, max_d, pct in REGULATORY:
        provision_expr = F.when(
            (F.col("dias_mora_act") >= min_d) & (F.col("dias_mora_act") <= max_d),
            F.lit(pct)
        ).otherwise(provision_expr)

    fact = (
        df
        .withColumn("bucket_mora", bucket_expr)
        .withColumn("calif_regulatoria", calif_expr)
        .withColumn("pct_provision", provision_expr)
        .withColumn("provision_estimada", F.round(F.col("sdo_capital") * F.col("pct_provision"), 0))
        .select(
            "id_oblig", "id_cli", "cod_prod",
            "vr_aprobado", "vr_desembolsado", "sdo_capital", "vr_cuota",
            "fec_desembolso", "fec_venc",
            "dias_mora_act", "bucket_mora",
            "calif_riesgo", "calif_regulatoria",
            "num_cuotas_pend",
            "pct_provision", "provision_estimada",
        )
        .withColumn("_gold_timestamp", F.lit(PROCESSING_TS).cast("timestamp"))
    )

    return write_gold(fact, "fact_cartera", partition_cols=["calif_regulatoria", "bucket_mora"])

# COMMAND ----------

# --- fact_rentabilidad_cliente (CLTV 12 meses) ---
def build_fact_rentabilidad():
    print("\n▶ Construyendo fact_rentabilidad_cliente...")
    df_mov = read_silver("silver_movimientos")
    df_com = read_silver("silver_comisiones")

    # # Usar la fecha máxima de los datos en vez de current_date
    max_date_mov = df_mov.agg(F.max("fec_mov")).collect()[0][0]
    cutoff_12m = F.lit(max_date_mov) - F.expr("INTERVAL 12 MONTHS")

    # Ingresos por movimientos (aprox.: tipo COMISION como proxy de intereses generados)
    mov_agg = (
        df_mov
        .filter(F.col("fec_mov") >= cutoff_12m)
        .filter(F.col("cod_estado_mov") == "APROBADO")
        .withColumn("periodo_year", F.year("fec_mov"))
        .withColumn("periodo_month", F.month("fec_mov"))
        .groupBy("id_cli", "periodo_year", "periodo_month")
        .agg(
            F.sum("vr_mov").alias("total_movimientos"),
            F.count("id_mov").alias("num_transacciones"),
        )
    )

    # Comisiones efectivamente cobradas
    com_agg = (
        df_com
        .filter(F.col("fec_cobro") >= cutoff_12m)
        .filter(F.col("estado_cobro") == "COBRADO")
        .withColumn("periodo_year", F.year("fec_cobro"))
        .withColumn("periodo_month", F.month("fec_cobro"))
        .groupBy("id_cli", "periodo_year", "periodo_month")
        .agg(
            F.sum("vr_comision").alias("total_comisiones_cobradas"),
            F.count("id_comision").alias("num_comisiones"),
        )
    )

    # Join y calcular CLTV
    fact = (
        mov_agg
        .join(com_agg, on=["id_cli", "periodo_year", "periodo_month"], how="full_outer")
        .fillna(0)
        .withColumn("ingreso_total",
            F.col("total_movimientos") * 0.015 + F.col("total_comisiones_cobradas"))
    )

    # CLTV acumulado 12 meses (rolling sum)
    window_12m = Window.partitionBy("id_cli").orderBy("periodo_year", "periodo_month").rowsBetween(-11, 0)
    fact = (
        fact
        .withColumn("cltv_12m", F.sum("ingreso_total").over(window_12m))
        .withColumn("_gold_timestamp", F.lit(PROCESSING_TS).cast("timestamp"))
    )

    return write_gold(fact, "fact_rentabilidad_cliente",
                      partition_cols=["periodo_year", "periodo_month"])

# COMMAND ----------

# =============================================================================
# TABLA DE KPIs EJECUTIVOS — Cartera diaria
# =============================================================================
def build_kpi_cartera_diaria():
    print("\n▶ Construyendo kpi_cartera_diaria...")
    df_cartera = spark.read.format("delta").load(f"{GOLD_PATH}/fact_cartera")
    df_clientes = spark.read.format("delta").load(f"{GOLD_PATH}/dim_clientes")

    # Enriquecer con segmento y ciudad del cliente
    df_enriched = df_cartera.join(
        df_clientes.select("id_cli", "segmento_desc", "ciudad_res"),
        on="id_cli", how="left"
    )

    kpi = (
        df_enriched
        .withColumn("fec_reporte", F.current_date())
        .groupBy("fec_reporte", "cod_prod", "segmento_desc", "ciudad_res")
        .agg(
            F.count("id_oblig").alias("total_obligaciones_activas"),
            F.sum("sdo_capital").alias("monto_total_cartera"),
            F.sum(F.when(F.col("dias_mora_act") > 0, F.col("sdo_capital")).otherwise(0))
                .alias("monto_en_mora"),
            F.countDistinct(
                F.when(F.col("dias_mora_act") > 0, F.col("id_cli"))
            ).alias("clientes_con_mora"),
            F.sum("provision_estimada").alias("total_provision_estimada"),
        )
        .withColumn("tasa_mora_pct",
            F.round(F.col("monto_en_mora") / F.col("monto_total_cartera") * 100, 2))
        .withColumn("_gold_timestamp", F.lit(PROCESSING_TS).cast("timestamp"))
    )

    return write_gold(kpi, "kpi_cartera_diaria")

# COMMAND ----------

# =============================================================================
# EJECUCIÓN PRINCIPAL
# =============================================================================
print("=" * 60)
print(f"GOLD PROCESSING — Batch: {BATCH_ID}")
print(f"Timestamp: {PROCESSING_TS}")
print("=" * 60)

start = time.time()

# Dimensiones
build_dim_clientes()
build_dim_productos()
build_dim_geografia_canal()

# Hechos
build_fact_transacciones()
build_fact_cartera()
build_fact_rentabilidad()

# KPIs ejecutivos
build_kpi_cartera_diaria()

duration = round(time.time() - start, 2)
print(f"\n{'=' * 60}")
print(f"✅ GOLD PROCESSING COMPLETADO en {duration}s")
print(f"{'=' * 60}")
