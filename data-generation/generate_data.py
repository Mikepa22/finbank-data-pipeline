"""
=============================================================================
FASE 1: Generación de Datos Sintéticos — FinBank S.A.
Escenario A: Banca y Servicios Financieros
=============================================================================
Genera datos sintéticos con distribuciones realistas, integridad referencial,
valores nulos controlados (~5%), anomalías intencionales documentadas,
y cobertura temporal de 12+ meses.

Salida: CSV y Parquet (ingesta heterogénea).
Reproducible: seed fija configurable vía YAML.

Uso:
    python generate_data.py --config ../config/pipeline_config.yaml
"""

import os
import sys
import argparse
import hashlib
import logging
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

# ===========================================================================
# CONFIGURACIÓN Y LOGGING
# ===========================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def load_config(config_path: str) -> dict:
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# ===========================================================================
# HELPERS
# ===========================================================================

def inject_nulls(df: pd.DataFrame, columns: list, pct: float = 0.05, rng: np.random.Generator = None):
    """Inyecta ~pct de nulos en columnas no-PK."""
    for col in columns:
        mask = rng.random(len(df)) < pct
        df.loc[mask, col] = None
    return df


def hash_doc(value: str) -> str:
    """SHA-256 parcial para enmascarar documento (usado solo en la generación como referencia)."""
    return hashlib.sha256(str(value).encode()).hexdigest()[:16]


# ===========================================================================
# GENERACIÓN DE TABLAS
# ===========================================================================

class FinBankDataGenerator:
    """Generador de datos sintéticos para el escenario FinBank."""

    def __init__(self, config: dict):
        self.cfg = config
        self.gen = config["data_generation"]
        self.seed = self.gen["seed"]
        self.rng = np.random.default_rng(self.seed)
        self.volumes = self.gen["volumes"]
        self.start_date = pd.Timestamp(self.gen["date_range"]["start"])
        self.end_date = pd.Timestamp(self.gen["date_range"]["end"])
        self.countries = self.gen["countries"]
        self.null_pct = self.gen["null_percentage"]

        # Colecciones intermedias
        self.clientes = None
        self.productos = None
        self.sucursales = None
        self.movimientos = None
        self.obligaciones = None
        self.comisiones = None

    # -------------------------------------------------------------------
    # TB_CLIENTES_CORE  (10,000 registros)
    # -------------------------------------------------------------------
    def generate_clientes(self) -> pd.DataFrame:
        n = self.volumes["TB_CLIENTES_CORE"]
        logger.info(f"Generando TB_CLIENTES_CORE — {n} registros")

        nombres_m = ["Carlos", "Juan", "Andrés", "Miguel", "Diego", "Santiago",
                      "Luis", "Pedro", "Daniel", "Alejandro", "Ricardo", "Fernando",
                      "José", "Sebastián", "Mateo", "David", "Nicolás", "Felipe"]
        nombres_f = ["María", "Ana", "Laura", "Carolina", "Valentina", "Camila",
                     "Daniela", "Sofía", "Gabriela", "Isabella", "Natalia", "Paula",
                     "Juliana", "Andrea", "Marcela", "Diana", "Lucía", "Elena"]
        apellidos = ["García", "Rodríguez", "Martínez", "López", "González",
                     "Hernández", "Pérez", "Sánchez", "Ramírez", "Torres",
                     "Flores", "Rivera", "Gómez", "Díaz", "Reyes", "Morales",
                     "Jiménez", "Ruiz", "Álvarez", "Romero", "Vargas", "Castro",
                     "Ortiz", "Mendoza", "Guerrero", "Medina", "Rojas", "Cruz"]

        # Distribución normal de edades centrada en 38 años
        ages = self.rng.normal(38, 12, n).clip(18, 75).astype(int)
        fec_nac = [self.end_date - pd.DateOffset(years=int(a)) - pd.DateOffset(
            days=int(self.rng.integers(0, 365))) for a in ages]

        # Distribución de segmentos: mayoría básico/estándar (pirámide)
        segmentos = self.rng.choice(
            ["01", "02", "03", "04"],
            size=n,
            p=[0.40, 0.35, 0.18, 0.07]
        )

        # Score buró: distribución normal ~650 con más dispersión en básico
        base_score = self.rng.normal(650, 120, n).clip(150, 950).astype(int)
        # Ajustar score según segmento
        seg_bonus = {"01": -50, "02": 0, "03": 50, "04": 100}
        scores = np.array([min(950, max(150, s + seg_bonus[seg])) for s, seg in zip(base_score, segmentos)])

        # Asignar país/ciudad con distribución realista (Colombia domina ~50%)
        country_weights = [0.50, 0.20, 0.12, 0.10, 0.08]
        country_indices = self.rng.choice(len(self.countries), size=n, p=country_weights)

        ciudades = []
        deptos = []
        for idx in country_indices:
            c = self.countries[idx]
            city_idx = self.rng.integers(0, len(c["cities"]))
            ciudades.append(c["cities"][city_idx])
            deptos.append(c["deptos"][min(city_idx, len(c["deptos"]) - 1)])

        canales = self.rng.choice(
            ["APP_MOVIL", "PORTAL_WEB", "CORRESPONSAL", "OFICINA", "REFERIDO"],
            size=n, p=[0.35, 0.25, 0.20, 0.12, 0.08]
        )

        # Género para elegir nombre
        genero = self.rng.choice(["M", "F"], size=n, p=[0.52, 0.48])
        noms = [self.rng.choice(nombres_m) if g == "M" else self.rng.choice(nombres_f) for g in genero]
        apels = [self.rng.choice(apellidos) + " " + self.rng.choice(apellidos) for _ in range(n)]

        df = pd.DataFrame({
            "id_cli": range(1, n + 1),
            "nomb_cli": noms,
            "apell_cli": apels,
            "tip_doc": self.rng.choice(["CC", "CE", "PA", "TI"], size=n, p=[0.80, 0.10, 0.07, 0.03]),
            "num_doc": [str(self.rng.integers(10000000, 99999999)) + str(self.rng.integers(10, 99)) for _ in range(n)],
            "fec_nac": fec_nac,
            "fec_alta": [self.start_date - pd.DateOffset(days=int(self.rng.integers(30, 2500))) for _ in range(n)],
            "cod_segmento": segmentos,
            "score_buro": scores,
            "ciudad_res": ciudades,
            "depto_res": deptos,
            "estado_cli": self.rng.choice(["ACTIVO", "INACTIVO", "BLOQUEADO", "RETIRADO"],
                                          size=n, p=[0.82, 0.08, 0.05, 0.05]),
            "canal_adquis": canales,
        })

        # Inyectar nulos controlados en campos no críticos
        df = inject_nulls(df, ["score_buro", "canal_adquis", "depto_res"], self.null_pct, self.rng)

        self.clientes = df
        logger.info(f"  ✓ TB_CLIENTES_CORE: {len(df)} registros generados")
        return df

    # -------------------------------------------------------------------
    # TB_PRODUCTOS_CAT  (50 registros)
    # -------------------------------------------------------------------
    def generate_productos(self) -> pd.DataFrame:
        n = self.volumes["TB_PRODUCTOS_CAT"]
        logger.info(f"Generando TB_PRODUCTOS_CAT — {n} registros")

        tipos = {
            "CREDITO_LIBRE_INVERSION": {"tasa_range": (0.12, 0.28), "plazo": (12, 84), "count": 12},
            "CREDITO_ROTATIVO": {"tasa_range": (0.22, 0.32), "plazo": (1, 36), "count": 8},
            "TARJETA_DIGITAL": {"tasa_range": (0.18, 0.30), "plazo": (1, 48), "count": 10},
            "CUENTA_AHORRO": {"tasa_range": (0.02, 0.08), "plazo": (0, 0), "count": 8},
            "PAGO_PSE": {"tasa_range": (0.0, 0.0), "plazo": (0, 0), "count": 4},
            "TRANSFERENCIA_ACH": {"tasa_range": (0.0, 0.0), "plazo": (0, 0), "count": 4},
            "CORRESPONSALIA": {"tasa_range": (0.0, 0.0), "plazo": (0, 0), "count": 4},
        }

        rows = []
        cod = 1
        for tip, meta in tipos.items():
            for i in range(meta["count"]):
                tasa = round(self.rng.uniform(*meta["tasa_range"]), 4) if meta["tasa_range"][1] > 0 else 0.0
                rows.append({
                    "cod_prod": f"PRD-{cod:03d}",
                    "desc_prod": f"{tip.replace('_', ' ').title()} {'Premium' if i % 3 == 0 else 'Estándar' if i % 3 == 1 else 'Básico'}",
                    "tip_prod": tip,
                    "tasa_ea": tasa,
                    "plazo_max_meses": int(self.rng.integers(meta["plazo"][0], meta["plazo"][1] + 1)) if meta["plazo"][1] > 0 else 0,
                    "cuota_min": round(self.rng.uniform(25000, 500000), 0) if tasa > 0 else 0,
                    "comision_admin": round(self.rng.uniform(0, 35000), 0),
                    "estado_prod": self.rng.choice(["ACTIVO", "INACTIVO"], p=[0.88, 0.12]),
                })
                cod += 1

        df = pd.DataFrame(rows[:n])
        self.productos = df
        logger.info(f"  ✓ TB_PRODUCTOS_CAT: {len(df)} registros generados")
        return df

    # -------------------------------------------------------------------
    # TB_SUCURSALES_RED  (200 registros)
    # -------------------------------------------------------------------
    def generate_sucursales(self) -> pd.DataFrame:
        n = self.volumes["TB_SUCURSALES_RED"]
        logger.info(f"Generando TB_SUCURSALES_RED — {n} registros")

        rows = []
        for i in range(1, n + 1):
            country = self.countries[self.rng.choice(len(self.countries), p=[0.45, 0.22, 0.13, 0.12, 0.08])]
            city_idx = self.rng.integers(0, len(country["cities"]))
            city = country["cities"][city_idx]
            depto = country["deptos"][min(city_idx, len(country["deptos"]) - 1)]
            tip = self.rng.choice(["OFICINA", "CORRESPONSAL", "DIGITAL", "KIOSCO"],
                                  p=[0.30, 0.40, 0.20, 0.10])

            # Latitudes y longitudes aproximadas para LATAM
            lat_base = {"Colombia": 4.6, "Mexico": 19.4, "Peru": -12.0, "Chile": -33.4, "Argentina": -34.6}
            lon_base = {"Colombia": -74.1, "Mexico": -99.1, "Peru": -77.0, "Chile": -70.6, "Argentina": -58.4}
            lat = lat_base.get(country["name"], 4.6) + self.rng.normal(0, 1.5)
            lon = lon_base.get(country["name"], -74.1) + self.rng.normal(0, 1.5)

            rows.append({
                "cod_suc": f"SUC-{i:04d}",
                "nom_suc": f"Sucursal {city} {i:03d}",
                "tip_punto": tip,
                "ciudad": city,
                "depto": depto,
                "latitud": round(lat, 6),
                "longitud": round(lon, 6),
                "activo": self.rng.choice([1, 0], p=[0.90, 0.10]),
            })

        df = pd.DataFrame(rows)
        self.sucursales = df
        logger.info(f"  ✓ TB_SUCURSALES_RED: {len(df)} registros generados")
        return df

    # -------------------------------------------------------------------
    # TB_MOV_FINANCIEROS  (500,000 registros + anomalías)
    # -------------------------------------------------------------------
    def generate_movimientos(self) -> pd.DataFrame:
        n = self.volumes["TB_MOV_FINANCIEROS"]
        logger.info(f"Generando TB_MOV_FINANCIEROS — {n} registros (+ anomalías)")

        client_ids = self.clientes["id_cli"].values
        prod_codes = self.productos["cod_prod"].values
        suc_codes = self.sucursales["cod_suc"].values
        ciudades_suc = self.sucursales["ciudad"].values

        # Distribución temporal: más transacciones en días hábiles, horarios pico
        total_days = (self.end_date - self.start_date).days
        dates = [self.start_date + pd.DateOffset(days=int(self.rng.integers(0, total_days))) for _ in range(n)]

        # Horas con distribución bimodal (picos a las 10am y 3pm)
        hours_mix = np.concatenate([
            self.rng.normal(10, 2, n // 2).clip(0, 23),
            self.rng.normal(15, 2, n - n // 2).clip(0, 23)
        ]).astype(int)
        self.rng.shuffle(hours_mix)
        minutes = self.rng.integers(0, 60, n)
        seconds = self.rng.integers(0, 60, n)

        # Montos: distribución log-normal (muchas transacciones pequeñas, pocas grandes)
        montos = np.round(self.rng.lognormal(mean=11.5, sigma=1.8, size=n).clip(1000, 50000000), 0)

        # Tipos de movimiento con distribución realista
        tipos_mov = self.rng.choice(
            ["PAGO", "TRANSFERENCIA", "RECARGA", "AVANCE", "COMPRA", "RETIRO", "DEPOSITO", "COMISION"],
            size=n,
            p=[0.25, 0.20, 0.12, 0.08, 0.15, 0.08, 0.07, 0.05]
        )

        canales = self.rng.choice(
            ["APP", "WEB", "CORRESPONSAL", "ATM", "OFICINA", "PSE"],
            size=n,
            p=[0.35, 0.25, 0.15, 0.10, 0.08, 0.07]
        )

        suc_idx = self.rng.integers(0, len(suc_codes), n)

        df = pd.DataFrame({
            "id_mov": range(1, n + 1),
            "id_cli": self.rng.choice(client_ids, size=n),
            "cod_prod": self.rng.choice(prod_codes, size=n),
            "num_cuenta": [f"CTA-{self.rng.integers(100000, 999999)}-{self.rng.integers(10, 99)}" for _ in range(n)],
            "fec_mov": dates,
            "hra_mov": [f"{h:02d}:{m:02d}:{s:02d}" for h, m, s in zip(hours_mix, minutes, seconds)],
            "vr_mov": montos,
            "tip_mov": tipos_mov,
            "cod_canal": canales,
            "cod_ciudad": ciudades_suc[suc_idx],
            "cod_estado_mov": self.rng.choice(
                ["APROBADO", "RECHAZADO", "PENDIENTE", "REVERSADO"],
                size=n, p=[0.88, 0.06, 0.04, 0.02]
            ),
            "id_dispositivo": [f"DEV-{self.rng.integers(100000, 999999)}" if self.rng.random() > 0.15 else None for _ in range(n)],
        })

        # --- ANOMALÍA 1: Transacciones duplicadas exactas (500) ---
        dup_indices = self.rng.integers(0, n, 500)
        duplicates = df.iloc[dup_indices].copy()
        duplicates["id_mov"] = range(n + 1, n + 501)
        df = pd.concat([df, duplicates], ignore_index=True)
        logger.info("  ⚠ Anomalía 1: 500 transacciones duplicadas inyectadas")

        # --- ANOMALÍA 2: Fechas futuras (200) ---
        future_idx = self.rng.integers(0, len(df), 200)
        for idx in future_idx:
            df.at[idx, "fec_mov"] = pd.Timestamp("2026-06-15") + pd.DateOffset(days=int(self.rng.integers(0, 180)))
        logger.info("  ⚠ Anomalía 2: 200 registros con fechas futuras inyectados")

        # Inyectar nulos controlados
        df = inject_nulls(df, ["id_dispositivo", "cod_ciudad"], self.null_pct, self.rng)

        self.movimientos = df
        logger.info(f"  ✓ TB_MOV_FINANCIEROS: {len(df)} registros generados (incluye anomalías)")
        return df

    # -------------------------------------------------------------------
    # TB_OBLIGACIONES  (30,000 registros)
    # -------------------------------------------------------------------
    def generate_obligaciones(self) -> pd.DataFrame:
        n = self.volumes["TB_OBLIGACIONES"]
        logger.info(f"Generando TB_OBLIGACIONES — {n} registros")

        client_ids = self.clientes["id_cli"].values
        prod_credito = self.productos[
            self.productos["tip_prod"].isin(["CREDITO_LIBRE_INVERSION", "CREDITO_ROTATIVO", "TARJETA_DIGITAL"])
        ]["cod_prod"].values

        # Montos aprobados: distribución log-normal
        vr_aprobado = np.round(self.rng.lognormal(mean=15.5, sigma=1.2, size=n).clip(500000, 200000000), 0)
        vr_desembolsado = np.round(vr_aprobado * self.rng.uniform(0.85, 1.0, n), 0)
        sdo_capital = np.round(vr_desembolsado * self.rng.uniform(0.1, 0.95, n), 0)

        # Días de mora: distribución exponencial (mayoría al día, cola hacia mora)
        dias_mora = np.round(self.rng.exponential(scale=15, size=n)).clip(0, 365).astype(int)
        # 60% al día
        al_dia_mask = self.rng.random(n) < 0.60
        dias_mora[al_dia_mask] = 0

        # Calificación de riesgo basada en mora
        def calc_calif(dias):
            if dias == 0: return "A"
            elif dias <= 30: return "B"
            elif dias <= 60: return "C"
            elif dias <= 90: return "D"
            else: return "E"

        califs = [calc_calif(d) for d in dias_mora]

        fec_desembolso = [self.start_date + pd.DateOffset(days=int(self.rng.integers(0, 365))) for _ in range(n)]
        fec_venc = [fd + pd.DateOffset(months=int(self.rng.integers(6, 72))) for fd in fec_desembolso]

        df = pd.DataFrame({
            "id_oblig": range(1, n + 1),
            "id_cli": self.rng.choice(client_ids, size=n),
            "cod_prod": self.rng.choice(prod_credito, size=n),
            "vr_aprobado": vr_aprobado,
            "vr_desembolsado": vr_desembolsado,
            "sdo_capital": sdo_capital,
            "vr_cuota": np.round(vr_aprobado / self.rng.integers(6, 72, n), 0),
            "fec_desembolso": fec_desembolso,
            "fec_venc": fec_venc,
            "dias_mora_act": dias_mora,
            "num_cuotas_pend": self.rng.integers(0, 72, n),
            "calif_riesgo": califs,
        })

        # --- ANOMALÍA 3: Registros inconsistentes ---
        # 150 con dias_mora negativo
        neg_idx = self.rng.integers(0, n, 150)
        for idx in neg_idx:
            df.at[idx, "dias_mora_act"] = -int(self.rng.integers(1, 30))
        # 100 con sdo_capital > vr_aprobado
        over_idx = self.rng.integers(0, n, 100)
        for idx in over_idx:
            df.at[idx, "sdo_capital"] = df.at[idx, "vr_aprobado"] * self.rng.uniform(1.1, 2.0)
        logger.info("  ⚠ Anomalía 3: 250 registros inconsistentes inyectados")

        df = inject_nulls(df, ["num_cuotas_pend", "vr_cuota"], self.null_pct, self.rng)

        self.obligaciones = df
        logger.info(f"  ✓ TB_OBLIGACIONES: {len(df)} registros generados")
        return df

    # -------------------------------------------------------------------
    # TB_COMISIONES_LOG  (80,000 registros)
    # -------------------------------------------------------------------
    def generate_comisiones(self) -> pd.DataFrame:
        n = self.volumes["TB_COMISIONES_LOG"]
        logger.info(f"Generando TB_COMISIONES_LOG — {n} registros")

        client_ids = self.clientes["id_cli"].values
        prod_codes = self.productos["cod_prod"].values

        total_days = (self.end_date - self.start_date).days

        df = pd.DataFrame({
            "id_comision": range(1, n + 1),
            "id_cli": self.rng.choice(client_ids, size=n),
            "cod_prod": self.rng.choice(prod_codes, size=n),
            "fec_cobro": [self.start_date + pd.DateOffset(days=int(self.rng.integers(0, total_days))) for _ in range(n)],
            "vr_comision": np.round(self.rng.lognormal(mean=9.5, sigma=1.0, size=n).clip(500, 500000), 0),
            "tip_comision": self.rng.choice(
                ["ADMIN", "SEGUROS", "MANTENIMIENTO", "MORA", "ESTUDIO_CREDITO", "PLATAFORMA"],
                size=n, p=[0.30, 0.15, 0.20, 0.15, 0.10, 0.10]
            ),
            "estado_cobro": self.rng.choice(
                ["COBRADO", "PENDIENTE", "REVERSO", "CONDONADO"],
                size=n, p=[0.78, 0.12, 0.05, 0.05]
            ),
        })

        df = inject_nulls(df, ["tip_comision"], self.null_pct, self.rng)

        self.comisiones = df
        logger.info(f"  ✓ TB_COMISIONES_LOG: {len(df)} registros generados")
        return df

    # -------------------------------------------------------------------
    # EXPORTAR DATOS
    # -------------------------------------------------------------------
    def export_data(self, output_dir: str, formats: list):
        """Exporta los datos generados en múltiples formatos."""
        tables = {
            "TB_CLIENTES_CORE": self.clientes,
            "TB_PRODUCTOS_CAT": self.productos,
            "TB_SUCURSALES_RED": self.sucursales,
            "TB_MOV_FINANCIEROS": self.movimientos,
            "TB_OBLIGACIONES": self.obligaciones,
            "TB_COMISIONES_LOG": self.comisiones,
        }

        for fmt in formats:
            fmt_dir = os.path.join(output_dir, fmt)
            os.makedirs(fmt_dir, exist_ok=True)

            for name, df in tables.items():
                if df is None:
                    continue
                filepath = os.path.join(fmt_dir, f"{name}.{fmt}")
                if fmt == "csv":
                    df.to_csv(filepath, index=False, encoding="utf-8")
                elif fmt == "parquet":
                    try:
                        df.to_parquet(filepath, index=False, engine="pyarrow")
                    except ImportError:
                        logger.warning(f"  ⚠ pyarrow no disponible, saltando Parquet para {name}")
                        continue
                logger.info(f"  💾 {name} → {filepath} ({len(df)} registros)")

    # -------------------------------------------------------------------
    # GENERAR RESUMEN
    # -------------------------------------------------------------------
    def generate_summary(self) -> dict:
        tables = {
            "TB_CLIENTES_CORE": self.clientes,
            "TB_PRODUCTOS_CAT": self.productos,
            "TB_SUCURSALES_RED": self.sucursales,
            "TB_MOV_FINANCIEROS": self.movimientos,
            "TB_OBLIGACIONES": self.obligaciones,
            "TB_COMISIONES_LOG": self.comisiones,
        }
        summary = {}
        for name, df in tables.items():
            if df is not None:
                null_pct = df.isnull().mean().to_dict()
                summary[name] = {
                    "row_count": len(df),
                    "columns": list(df.columns),
                    "null_percentages": {k: round(v, 4) for k, v in null_pct.items() if v > 0},
                    "dtypes": df.dtypes.astype(str).to_dict(),
                }
        return summary

    # -------------------------------------------------------------------
    # EJECUTAR TODO
    # -------------------------------------------------------------------
    def run(self, output_dir: str):
        logger.info("=" * 60)
        logger.info("INICIO - Generación de datos sintéticos FinBank S.A.")
        logger.info(f"Seed: {self.seed} | Rango: {self.gen['date_range']}")
        logger.info("=" * 60)

        self.generate_clientes()
        self.generate_productos()
        self.generate_sucursales()
        self.generate_movimientos()
        self.generate_obligaciones()
        self.generate_comisiones()

        formats = self.gen.get("output_formats", ["csv", "parquet"])
        self.export_data(output_dir, formats)

        summary = self.generate_summary()
        logger.info("\n" + "=" * 60)
        logger.info("RESUMEN DE GENERACIÓN")
        logger.info("=" * 60)
        for table, info in summary.items():
            logger.info(f"  {table}: {info['row_count']} registros")
        logger.info("=" * 60)
        logger.info("GENERACIÓN COMPLETADA EXITOSAMENTE")

        # Guardar resumen como JSON
        import json
        summary_path = os.path.join(output_dir, "generation_summary.json")
        with open(summary_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, default=str)
        logger.info(f"  📋 Resumen guardado en {summary_path}")

        return summary


# ===========================================================================
# MAIN
# ===========================================================================
def main():
    parser = argparse.ArgumentParser(description="Generador de datos sintéticos FinBank S.A.")
    parser.add_argument("--config", type=str, default="../config/pipeline_config.yaml",
                        help="Ruta al archivo de configuración YAML")
    parser.add_argument("--output", type=str, default="./output",
                        help="Directorio de salida para los datos generados")
    args = parser.parse_args()

    config = load_config(args.config)
    generator = FinBankDataGenerator(config)
    generator.run(args.output)


if __name__ == "__main__":
    main()
