"""
=============================================================================
Carga de datos sintéticos a SQL Database (Azure SQL / PostgreSQL / SQLite)
=============================================================================
Lee los CSV generados y los carga en la base de datos relacional que
actúa como fuente transaccional del pipeline.

Uso:
    python load_to_db.py --config ../config/pipeline_config.yaml --data-dir ./output/csv

Para pruebas locales (sin Azure):
    python load_to_db.py --config ../config/pipeline_config.yaml --data-dir ./output/csv --local
"""

import os
import sys
import argparse
import logging
from pathlib import Path

import pandas as pd
import yaml

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_engine(config: dict, local: bool = False):
    """Crea el engine SQLAlchemy según la plataforma."""
    from sqlalchemy import create_engine

    if local:
        # SQLite local para pruebas
        db_path = "finbank_transactional.db"
        logger.info(f"Usando SQLite local: {db_path}")
        return create_engine(f"sqlite:///{db_path}")

    # Azure SQL Database
    infra = config["infrastructure"]["azure"]
    server = f"{infra['sql_database']['server_name']}-{infra['environment']}.database.windows.net"
    database = infra["sql_database"]["database_name"]
    username = infra["sql_database"]["admin_login"]

    # La contraseña viene de variable de entorno (nunca en código)
    password = os.environ.get("SQL_ADMIN_PASSWORD")
    if not password:
        logger.error("Variable SQL_ADMIN_PASSWORD no configurada")
        sys.exit(1)

    conn_str = (
        f"mssql+pyodbc://{username}:{password}@{server}/{database}"
        f"?driver=ODBC+Driver+18+for+SQL+Server&Encrypt=yes&TrustServerCertificate=no"
    )
    logger.info(f"Conectando a Azure SQL: {server}/{database}")
    return create_engine(conn_str)


def create_tables_ddl(engine, dialect: str = "sqlite"):
    """Crea las tablas con DDL explícito para integridad referencial."""
    from sqlalchemy import text

    ddl_statements = [
        """
        CREATE TABLE IF NOT EXISTS TB_CLIENTES_CORE (
            id_cli INTEGER PRIMARY KEY,
            nomb_cli VARCHAR(100),
            apell_cli VARCHAR(150),
            tip_doc VARCHAR(5),
            num_doc VARCHAR(20),
            fec_nac DATE,
            fec_alta DATE,
            cod_segmento VARCHAR(5),
            score_buro INTEGER,
            ciudad_res VARCHAR(100),
            depto_res VARCHAR(100),
            estado_cli VARCHAR(20),
            canal_adquis VARCHAR(20)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS TB_PRODUCTOS_CAT (
            cod_prod VARCHAR(10) PRIMARY KEY,
            desc_prod VARCHAR(200),
            tip_prod VARCHAR(50),
            tasa_ea REAL,
            plazo_max_meses INTEGER,
            cuota_min REAL,
            comision_admin REAL,
            estado_prod VARCHAR(20)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS TB_SUCURSALES_RED (
            cod_suc VARCHAR(10) PRIMARY KEY,
            nom_suc VARCHAR(200),
            tip_punto VARCHAR(30),
            ciudad VARCHAR(100),
            depto VARCHAR(100),
            latitud REAL,
            longitud REAL,
            activo INTEGER
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS TB_MOV_FINANCIEROS (
            id_mov BIGINT PRIMARY KEY,
            id_cli INTEGER,
            cod_prod VARCHAR(10),
            num_cuenta VARCHAR(30),
            fec_mov DATE,
            hra_mov VARCHAR(10),
            vr_mov REAL,
            tip_mov VARCHAR(30),
            cod_canal VARCHAR(20),
            cod_ciudad VARCHAR(100),
            cod_estado_mov VARCHAR(20),
            id_dispositivo VARCHAR(20)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS TB_OBLIGACIONES (
            id_oblig INTEGER PRIMARY KEY,
            id_cli INTEGER,
            cod_prod VARCHAR(10),
            vr_aprobado REAL,
            vr_desembolsado REAL,
            sdo_capital REAL,
            vr_cuota REAL,
            fec_desembolso DATE,
            fec_venc DATE,
            dias_mora_act INTEGER,
            num_cuotas_pend INTEGER,
            calif_riesgo VARCHAR(5)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS TB_COMISIONES_LOG (
            id_comision INTEGER PRIMARY KEY,
            id_cli INTEGER,
            cod_prod VARCHAR(10),
            fec_cobro DATE,
            vr_comision REAL,
            tip_comision VARCHAR(30),
            estado_cobro VARCHAR(20)
        )
        """,
    ]

    with engine.begin() as conn:
        for ddl in ddl_statements:
            conn.execute(text(ddl))
    logger.info("✅ Tablas DDL creadas exitosamente")


def load_table(engine, table_name: str, csv_path: str):
    """Carga un CSV a la tabla SQL usando pandas to_sql."""
    df = pd.read_csv(csv_path, low_memory=False)
    logger.info(f"  Cargando {table_name}: {len(df)} registros desde {csv_path}")

    df.to_sql(
        name=table_name,
        con=engine,
        if_exists="replace",
        index=False,
        chunksize=5000,
    )
    logger.info(f"  ✅ {table_name}: {len(df)} registros cargados")
    return len(df)


def verify_counts(engine):
    """Ejecuta SELECT COUNT(*) por tabla para evidencia."""
    from sqlalchemy import text

    tables = [
        "TB_CLIENTES_CORE", "TB_PRODUCTOS_CAT", "TB_SUCURSALES_RED",
        "TB_MOV_FINANCIEROS", "TB_OBLIGACIONES", "TB_COMISIONES_LOG"
    ]

    print("\n" + "=" * 50)
    print("VERIFICACIÓN DE CARGA — SELECT COUNT(*)")
    print("=" * 50)
    with engine.connect() as conn:
        for table in tables:
            try:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                count = result.scalar()
                print(f"  {table}: {count:,} registros")
            except Exception as e:
                print(f"  {table}: ERROR — {e}")
    print("=" * 50)


def main():
    parser = argparse.ArgumentParser(description="Carga de datos a SQL Database")
    parser.add_argument("--config", type=str, default="../config/pipeline_config.yaml")
    parser.add_argument("--data-dir", type=str, default="./output/csv")
    parser.add_argument("--local", action="store_true", help="Usar SQLite local")
    args = parser.parse_args()

    config = load_config(args.config)
    engine = get_engine(config, local=args.local)

    # Crear tablas
    create_tables_ddl(engine)

    # Cargar datos
    tables = [
        "TB_CLIENTES_CORE", "TB_PRODUCTOS_CAT", "TB_SUCURSALES_RED",
        "TB_MOV_FINANCIEROS", "TB_OBLIGACIONES", "TB_COMISIONES_LOG"
    ]

    total = 0
    for table in tables:
        csv_path = os.path.join(args.data_dir, f"{table}.csv")
        if os.path.exists(csv_path):
            total += load_table(engine, table, csv_path)
        else:
            logger.warning(f"  ⚠ {csv_path} no encontrado, saltando")

    # Verificar
    verify_counts(engine)
    logger.info(f"\n✅ Carga completada: {total:,} registros totales")


if __name__ == "__main__":
    main()
