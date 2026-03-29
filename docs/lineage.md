# Linaje de Datos — Campos Calculados en Gold

## 1. `fact_cartera.provision_estimada`

**Propósito**: Estimar la provisión regulatoria que FinBank debe constituir por cada obligación según la normativa de la Superintendencia Financiera de Colombia.

| Paso | Capa | Transformación |
|------|------|----------------|
| 1 | Bronze | `TB_OBLIGACIONES.dias_mora_act` ingestado tal cual desde SQL Server |
| 2 | Silver | Validación: rechazar registros con `dias_mora_act < 0` (anomalía 3). Validar integridad referencial contra `silver_clientes`. Cast a `INT`. |
| 3 | Gold | Clasificar `dias_mora_act` en `bucket_mora` (Al día, Rango 1-3, Deteriorado). Asignar `calif_regulatoria` (A/B/C/D/E). Buscar `pct_provision` según tabla regulatoria parametrizada. |
| 4 | Gold | `provision_estimada = sdo_capital × pct_provision` |

**Origen**: `TB_OBLIGACIONES.sdo_capital` + `TB_OBLIGACIONES.dias_mora_act`  
**Tabla regulatoria** (parametrizada en `pipeline_config.yaml`):

| Calificación | Mora (días) | % Provisión |
|-------------|-------------|-------------|
| A | 0 | 1% |
| B | 1-30 | 3.2% |
| C | 31-60 | 11.7% |
| D | 61-90 | 50% |
| E | >90 | 100% |

---

## 2. `silver_movimientos.ind_sospechoso`

**Propósito**: Marcar transacciones con comportamiento atípico respecto al histórico del cliente para alimentar las reglas del motor de fraude.

| Paso | Capa | Transformación |
|------|------|----------------|
| 1 | Bronze | `TB_MOV_FINANCIEROS.vr_mov`, `id_cli`, `fec_mov` ingestados sin modificación |
| 2 | Silver (dedup) | Eliminar 500 duplicados exactos (Anomalía 1). Eliminar 200 registros con fecha futura (Anomalía 2). |
| 3 | Silver (calc) | Window function: `AVG(vr_mov)` y `STDDEV(vr_mov)` sobre ventana de 30 días particionada por `id_cli`, ordenada por `fec_mov` |
| 4 | Silver (flag) | `ind_sospechoso = 1` cuando `vr_mov > avg_30d + 3 × std_30d` |
| 5 | Gold | Se hereda directamente en `fact_transacciones.ind_sospechoso` |

**Origen**: `TB_MOV_FINANCIEROS.vr_mov`  
**Parámetros**: Umbral = 3σ, ventana = 30 días (parametrizados en config)

---

## 3. `fact_rentabilidad_cliente.cltv_12m`

**Propósito**: Calcular el Customer Lifetime Value mensual para cada cliente, integrando ingresos por intereses estimados y comisiones efectivamente cobradas en los últimos 12 meses.

| Paso | Capa | Transformación |
|------|------|----------------|
| 1 | Bronze | `TB_MOV_FINANCIEROS` y `TB_COMISIONES_LOG` ingestados como datos crudos |
| 2 | Silver | Limpieza, deduplicación y validación de integridad referencial en ambas tablas |
| 3 | Gold (agg mov) | Agrupar `silver_movimientos` por `id_cli`, año y mes. Filtrar últimos 12 meses y estado APROBADO. Calcular `total_movimientos = SUM(vr_mov)`. |
| 4 | Gold (agg com) | Agrupar `silver_comisiones` por `id_cli`, año y mes. Filtrar últimos 12 meses y estado COBRADO. Calcular `total_comisiones_cobradas = SUM(vr_comision)`. |
| 5 | Gold (join) | `FULL OUTER JOIN` por `id_cli`, año, mes. `ingreso_total = total_movimientos × 0.015 + total_comisiones_cobradas` |
| 6 | Gold (rolling) | `cltv_12m = SUM(ingreso_total) OVER (PARTITION BY id_cli ORDER BY periodo ROWS BETWEEN 11 PRECEDING AND CURRENT ROW)` |

**Origen**: `TB_MOV_FINANCIEROS.vr_mov` + `TB_COMISIONES_LOG.vr_comision`  
**Supuesto**: El 1.5% del volumen transaccional se usa como proxy de ingreso por intereses. En producción se usaría el cálculo real de spread financiero.

---

## 4. `kpi_cartera_diaria.tasa_mora_pct`

**Propósito**: KPI ejecutivo diario que muestra el porcentaje de cartera en mora por producto, segmento y ciudad.

| Paso | Capa | Transformación |
|------|------|----------------|
| 1 | Gold | Leer `fact_cartera` y `dim_clientes` |
| 2 | Gold | JOIN por `id_cli` para enriquecer con segmento y ciudad |
| 3 | Gold | Agrupar por fecha, producto, segmento, ciudad |
| 4 | Gold | `tasa_mora_pct = (monto_en_mora / monto_total_cartera) × 100` |

**Origen**: `fact_cartera.sdo_capital` + `fact_cartera.dias_mora_act` + `dim_clientes.segmento_desc`
