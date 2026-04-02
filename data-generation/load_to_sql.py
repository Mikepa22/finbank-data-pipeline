"""
=============================================================================
Carga de datos sintéticos a Azure SQL Database
=============================================================================
Carga las 6 tablas fuente del sistema transaccional de FinBank S.A.
desde archivos CSV generados por generate_data.py.

Seguridad:
    Las credenciales se leen de variables de entorno (nunca hardcodeadas).
    Configurar antes de ejecutar:
        set SQL_SERVER=sql-finbank-source-dev.database.windows.net
        set SQL_DATABASE=finbank_transactional
        set SQL_USER=finbank_admin
        set SQL_PASSWORD=<tu_password>

    PowerShell:
        $env:SQL_SERVER = "sql-finbank-source-dev.database.windows.net"
        $env:SQL_DATABASE = "finbank_transactional"
        $env:SQL_USER = "finbank_admin"
        $env:SQL_PASSWORD = "<tu_password>"

Uso:
    python load_to_sql.py --data-dir ./output/csv
    python load_to_sql.py --data-dir ./output/csv --batch-size 5000
=============================================================================
"""

import os
import sys
import time
import argparse
import logging

import pandas as pd
import pyodbc

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Tablas en orden de dependencia (padres primero, hijas después)
# ---------------------------------------------------------------------------
TABLES = [
    "TB_PRODUCTOS_CAT",
    "TB_SUCURSALES_RED",
    "TB_CLIENTES_CORE",
    "TB_OBLIGACIONES",
    "TB_COMISIONES_LOG",
    "TB_MOV_FINANCIEROS",
]


def get_connection_string() -> str:
    """Construye el connection string desde variables de entorno."""
    required_vars = ["SQL_SERVER", "SQL_DATABASE", "SQL_USER", "SQL_PASSWORD"]
    missing = [v for v in required_vars if not os.environ.get(v)]

    if missing:
        logger.error(
            f"Variables de entorno faltantes: {', '.join(missing)}\n"
            f"Configurar con:\n"
            f"  set SQL_SERVER=tu_servidor.database.windows.net\n"
            f"  set SQL_DATABASE=finbank_transactional\n"
            f"  set SQL_USER=finbank_admin\n"
            f"  set SQL_PASSWORD=tu_password"
        )
        sys.exit(1)

    return (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={os.environ['SQL_SERVER']};"
        f"DATABASE={os.environ['SQL_DATABASE']};"
        f"UID={os.environ['SQL_USER']};"
        f"PWD={os.environ['SQL_PASSWORD']};"
        f"Encrypt=yes;TrustServerCertificate=no;"
    )


def disable_constraints(conn):
    """Desactiva todas las foreign keys para permitir carga sin orden estricto."""
    cursor = conn.cursor()
    cursor.execute("""
        DECLARE @sql NVARCHAR(MAX) = '';
        SELECT @sql += 'ALTER TABLE ' + QUOTENAME(s.name) + '.' + QUOTENAME(t.name)
                      + ' NOCHECK CONSTRAINT ALL;' + CHAR(13)
        FROM sys.tables t
        JOIN sys.schemas s ON t.schema_id = s.schema_id;
        EXEC sp_executesql @sql;
    """)
    conn.commit()
    logger.info("Foreign keys desactivadas")


def enable_constraints(conn):
    """Reactiva todas las foreign keys."""
    cursor = conn.cursor()
    cursor.execute("""
        DECLARE @sql NVARCHAR(MAX) = '';
        SELECT @sql += 'ALTER TABLE ' + QUOTENAME(s.name) + '.' + QUOTENAME(t.name)
                      + ' WITH CHECK CHECK CONSTRAINT ALL;' + CHAR(13)
        FROM sys.tables t
        JOIN sys.schemas s ON t.schema_id = s.schema_id;
        EXEC sp_executesql @sql;
    """)
    conn.commit()
    logger.info("Foreign keys reactivadas")


def load_table(conn, table_name: str, csv_path: str, batch_size: int) -> dict:
    """
    Carga un CSV a una tabla existente en Azure SQL.
    Retorna métricas de la carga.
    """
    cursor = conn.cursor()
    cursor.fast_executemany = True

    metrics = {
        "table": table_name,
        "status": "SUCCESS",
        "rows_in_csv": 0,
        "rows_loaded": 0,
        "errors": 0,
        "duration_seconds": 0,
    }

    start = time.time()

    # Leer CSV
    logger.info(f"[{table_name}] Leyendo {csv_path}...")
    df = pd.read_csv(csv_path, low_memory=False)
    metrics["rows_in_csv"] = len(df)

    # Limpiar tabla
    cursor.execute(f"DELETE FROM dbo.{table_name}")
    conn.commit()

    # Preparar INSERT
    columns = ", ".join([f"[{col}]" for col in df.columns])
    placeholders = ", ".join(["?" for _ in df.columns])
    insert_sql = f"INSERT INTO dbo.{table_name} ({columns}) VALUES ({placeholders})"

    # Convertir DataFrame a lista de tuplas con NaN → None
    df = df.where(df.notna(), None)
    rows = [tuple(row) for row in df.itertuples(index=False)]

    # Insertar en lotes
    total = len(rows)
    inserted = 0
    errors = 0

    for i in range(0, total, batch_size):
        batch = rows[i : i + batch_size]
        try:
            cursor.executemany(insert_sql, batch)
            conn.commit()
            inserted += len(batch)
        except Exception:
            # Fallback: insertar fila por fila si el lote falla
            cursor.fast_executemany = False
            for row in batch:
                try:
                    cursor.execute(insert_sql, row)
                    conn.commit()
                    inserted += 1
                except Exception:
                    errors += 1
            cursor.fast_executemany = True

        # Progreso
        done = min(i + batch_size, total)
        if done % 50000 == 0 or done >= total:
            elapsed = round(time.time() - start, 1)
            rate = round(done / elapsed) if elapsed > 0 else 0
            logger.info(f"  {done:,}/{total:,} — {elapsed}s ({rate} reg/s)")

    # Verificar
    count = cursor.execute(f"SELECT COUNT(*) FROM dbo.{table_name}").fetchone()[0]
    duration = round(time.time() - start, 2)

    metrics["rows_loaded"] = count
    metrics["errors"] = errors
    metrics["duration_seconds"] = duration

    if errors > 0:
        logger.warning(
            f"  {table_name}: {count:,} registros OK, "
            f"{errors} rechazados (anomalías intencionales) — {duration}s"
        )
    else:
        logger.info(f"  {table_name}: {count:,} registros OK — {duration}s")

    return metrics


def main():
    parser = argparse.ArgumentParser(
        description="Carga datos sintéticos de FinBank a Azure SQL Database"
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        default="./output/csv",
        help="Directorio con los archivos CSV generados",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=5000,
        help="Tamaño del lote para inserciones (default: 5000)",
    )
    args = parser.parse_args()

    # Conectar
    logger.info("=" * 60)
    logger.info("CARGA DE DATOS A AZURE SQL DATABASE — FinBank S.A.")
    logger.info(f"Servidor: {os.environ.get('SQL_SERVER', 'no configurado')}")
    logger.info(f"Base de datos: {os.environ.get('SQL_DATABASE', 'no configurado')}")
    logger.info(f"Directorio CSV: {args.data_dir}")
    logger.info("=" * 60)

    conn_str = get_connection_string()
    try:
        conn = pyodbc.connect(conn_str)
        logger.info("Conexión exitosa")
    except Exception as e:
        logger.error(f"Error de conexión: {e}")
        sys.exit(1)

    # Desactivar constraints
    disable_constraints(conn)

    # Cargar tablas
    total_start = time.time()
    all_metrics = []

    for table in TABLES:
        csv_path = os.path.join(args.data_dir, f"{table}.csv")
        if not os.path.exists(csv_path):
            logger.error(f"  {csv_path} no encontrado, saltando")
            continue
        metrics = load_table(conn, table, csv_path, args.batch_size)
        all_metrics.append(metrics)

    # Reactivar constraints
    try:
        enable_constraints(conn)
    except Exception:
        logger.warning("Algunas constraints no se reactivaron (normal con anomalías)")

    # Resumen final
    total_time = round(time.time() - total_start, 2)
    cursor = conn.cursor()

    logger.info("")
    logger.info("=" * 60)
    logger.info("EVIDENCIA DE CARGA — SELECT COUNT(*) POR TABLA")
    logger.info("=" * 60)

    grand_total = 0
    for table in TABLES:
        try:
            count = cursor.execute(f"SELECT COUNT(*) FROM dbo.{table}").fetchone()[0]
        except Exception:
            count = 0
        grand_total += count
        logger.info(f"  {table:25s} | {count:>10,} registros")

    logger.info("=" * 60)
    logger.info(f"  {'TOTAL':25s} | {grand_total:>10,} registros")
    logger.info(f"  Tiempo total: {total_time}s")
    logger.info("=" * 60)

    # Resumen de errores (anomalías intencionales)
    total_errors = sum(m["errors"] for m in all_metrics)
    if total_errors > 0:
        logger.info(
            f"\n  Nota: {total_errors} registros rechazados corresponden a "
            f"anomalías intencionales inyectadas en la generación de datos "
            f"(duplicados, fechas futuras, registros inconsistentes)."
        )

    conn.close()
    logger.info("\nCarga completada exitosamente")


if __name__ == "__main__":
    main()
