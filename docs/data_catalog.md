# Catálogo de Datos — FinBank Pipeline

## Capa Silver

### silver_clientes
| Campo | Tipo | Origen | Sensible | Descripción |
|-------|------|--------|----------|-------------|
| id_cli | INT | TB_CLIENTES_CORE.id_cli | No | Identificador único del cliente |
| num_doc_hash | STRING | SHA256(TB_CLIENTES_CORE.num_doc) | PII-Masked | Hash del número de documento |
| nomb_cli_hash | STRING | SHA256(TB_CLIENTES_CORE.nomb_cli) | PII-Masked | Hash del nombre |
| apell_cli_hash | STRING | SHA256(TB_CLIENTES_CORE.apell_cli) | PII-Masked | Hash del apellido |
| tip_doc | STRING | TB_CLIENTES_CORE.tip_doc | No | Tipo de documento (CC, CE, PA, TI) |
| fec_nac | DATE | TB_CLIENTES_CORE.fec_nac | PII | Fecha de nacimiento |
| fec_alta | DATE | TB_CLIENTES_CORE.fec_alta | No | Fecha de vinculación |
| cod_segmento | STRING | TB_CLIENTES_CORE.cod_segmento | No | Código de segmento (01-04) |
| score_buro | INT | TB_CLIENTES_CORE.score_buro | Financiero | Score de buró crediticio |
| ciudad_res | STRING | TB_CLIENTES_CORE.ciudad_res | No | Ciudad de residencia (normalizada) |
| depto_res | STRING | TB_CLIENTES_CORE.depto_res | No | Departamento de residencia |
| estado_cli | STRING | TB_CLIENTES_CORE.estado_cli | No | Estado del cliente |
| canal_adquis | STRING | TB_CLIENTES_CORE.canal_adquis | No | Canal de adquisición |

### silver_movimientos
| Campo | Tipo | Origen | Sensible | Descripción |
|-------|------|--------|----------|-------------|
| id_mov | LONG | TB_MOV_FINANCIEROS.id_mov | No | ID del movimiento |
| id_cli | INT | TB_MOV_FINANCIEROS.id_cli | No | FK a silver_clientes |
| cod_prod | STRING | TB_MOV_FINANCIEROS.cod_prod | No | FK a silver_productos |
| fec_mov | DATE | TB_MOV_FINANCIEROS.fec_mov | No | Fecha del movimiento |
| vr_mov | DOUBLE | TB_MOV_FINANCIEROS.vr_mov | Financiero | Valor del movimiento (COP) |
| flag_horario | STRING | Calculado (hra_mov) | No | HABIL / NO_HABIL |
| ind_sospechoso | INT | Calculado (3σ rolling 30d) | No | 1 si supera 3 desv. estándar |
| avg_30d | DOUBLE | Calculado | No | Promedio móvil 30 días del cliente |
| std_30d | DOUBLE | Calculado | No | Desviación estándar 30 días |
| id_dispositivo_hash | STRING | SHA256(id_dispositivo) | PII-Masked | Hash del dispositivo |

### silver_obligaciones
| Campo | Tipo | Origen | Sensible | Descripción |
|-------|------|--------|----------|-------------|
| id_oblig | INT | TB_OBLIGACIONES.id_oblig | No | ID de la obligación |
| id_cli | INT | TB_OBLIGACIONES.id_cli | No | FK a silver_clientes |
| sdo_capital | DOUBLE | TB_OBLIGACIONES.sdo_capital | Financiero | Saldo de capital vigente |
| dias_mora_act | INT | TB_OBLIGACIONES.dias_mora_act | No | Días de mora actuales (≥0) |
| sdo_capital_masked | DOUBLE | sdo_capital / 1M | Financiero-Masked | Saldo en millones |

---

## Capa Gold

### dim_clientes
| Campo | Tipo | Origen | Descripción |
|-------|------|--------|-------------|
| id_cli | INT | silver_clientes | PK — Identificador del cliente |
| edad | INT | Calculado: (hoy - fec_nac) / 365.25 | Edad actual |
| grupo_edad | STRING | Calculado desde edad | Rango: 18-24, 25-34, ..., 65+ |
| segmento_desc | STRING | Mapeado de cod_segmento | Básico, Estándar, Premium, Elite |
| antiguedad_dias | INT | Calculado: hoy - fec_alta | Días desde vinculación |

### dim_productos
| Campo | Tipo | Origen | Descripción |
|-------|------|--------|-------------|
| producto_id | STRING | silver_productos.cod_prod | PK |
| tasa_mensual_equivalente | DOUBLE | Calculado: (1+tasa_ea)^(1/12)-1 | Tasa mensual |
| familia_producto | STRING | Clasificado desde tip_prod | Crédito, Ahorro, Transaccional |

### fact_cartera
| Campo | Tipo | Origen | Descripción |
|-------|------|--------|-------------|
| bucket_mora | STRING | Clasificado desde dias_mora_act | Al día, Rango 1-3, Deteriorado |
| calif_regulatoria | STRING | Calculado desde dias_mora_act | A/B/C/D/E según normativa SFC |
| provision_estimada | DOUBLE | sdo_capital × pct_provision | Provisión según tabla regulatoria |

### fact_transacciones
| Campo | Tipo | Origen | Descripción |
|-------|------|--------|-------------|
| vr_mov_usd | DOUBLE | vr_mov × FX_COP_USD | Monto en USD |
| ind_sospechoso | INT | Silver (3σ) | Flag de transacción atípica |

### fact_rentabilidad_cliente
| Campo | Tipo | Origen | Descripción |
|-------|------|--------|-------------|
| ingreso_total | DOUBLE | movimientos×1.5% + comisiones | Ingreso mensual por cliente |
| cltv_12m | DOUBLE | SUM(ingreso_total) rolling 12m | Customer Lifetime Value |

### kpi_cartera_diaria
| Campo | Tipo | Descripción |
|-------|------|-------------|
| total_obligaciones_activas | LONG | Conteo de obligaciones |
| monto_total_cartera | DOUBLE | Suma de sdo_capital |
| monto_en_mora | DOUBLE | Cartera con dias_mora > 0 |
| tasa_mora_pct | DOUBLE | (monto_mora / monto_total) × 100 |
| clientes_con_mora | LONG | Clientes distintos con alguna mora |
