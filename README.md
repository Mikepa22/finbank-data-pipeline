# FinBank S.A. — End-to-End Data Pipeline

## Sector Elegido: **Escenario A — Banca y Servicios Financieros**
## Plataforma Cloud: **Microsoft Azure**
## Herramienta IaC: **Terraform**

---

## Justificación de Decisiones Técnicas

### ¿Por qué Escenario A (Banca)?
Mi experiencia profesional incluye trabajo directo en el sector financiero (Bancoomeva), donde implementé pipelines de datos con ADF, PySpark y arquitectura medallion sobre Azure. El dominio del contexto regulatorio colombiano (Superintendencia Financiera), provisiones de cartera, y detección de anomalías financieras me permite aportar valor más allá de lo técnico, incorporando lógica de negocio realista en cada transformación.

### ¿Por qué Microsoft Azure?
- **Certificaciones activas**: PL-300 (Power BI), DP-600 (Fabric Analytics), DP-700 (Azure Data Engineering)
- **Experiencia comprobada**: Migración on-prem a Azure en Bancoomeva con ADF + Databricks + ADLS Gen2
- **Ecosistema integrado**: Azure ofrece el stack más cohesivo para este escenario (Key Vault + RBAC nativo + Log Analytics + Action Groups)
- **Delta Lake nativo**: Databricks sobre Azure soporta Delta Lake sin configuración adicional, habilitando ACID, time travel y MERGE para idempotencia

### ¿Por qué Terraform (no Bicep)?
- **Multi-cloud portable**: Aunque esta solución es Azure, Terraform facilita extensión futura a GCP o AWS
- **State management maduro**: Backend remoto en Azure Storage con locking integrado
- **Ecosistema de módulos**: Provider oficial de Databricks con soporte completo

### ¿Por qué Delta Lake (no Parquet plano)?
- **Idempotencia**: `MERGE` nativo elimina el riesgo de duplicados en re-ejecuciones
- **Time travel**: Permite auditoría y rollback sin infraestructura adicional
- **Schema enforcement**: Evita corrupción de datos por cambios no controlados
- **OPTIMIZE + ZORDER**: Performance superior para las consultas analíticas de Gold

---

## Arquitectura de la Solución

```
┌──────────────────────────────────────────────────────────────┐
│                    AZURE DATA FACTORY                        │
│              (Trigger diario 02:00 COT)                      │
│                         │                                    │
│    ┌────────────────────┼────────────────────┐               │
│    │        DATABRICKS WORKFLOW (DAG)        │               │
│    │                                        │               │
│    │  ┌─────────┐  ┌─────────┐  ┌────────┐ │               │
│    │  │ BRONZE  │→ │ SILVER  │→ │  GOLD  │ │               │
│    │  │ Ingesta │  │ Limpieza│  │ Reglas │ │               │
│    │  │ cruda   │  │ PII mask│  │negocio │ │               │
│    │  │ Delta   │  │ Validac │  │ Dims + │ │               │
│    │  │ Lake    │  │ Quality │  │ Facts  │ │               │
│    │  └─────────┘  └─────────┘  └────────┘ │               │
│    │       ↓              ↓           ↓     │               │
│    │  ┌──────────────────────────────────┐  │               │
│    │  │   ADLS Gen2 (OneLake)            │  │               │
│    │  │   /bronze  /silver  /gold        │  │               │
│    │  │   /errors  /logs                 │  │               │
│    │  └──────────────────────────────────┘  │               │
│    └────────────────────────────────────────┘               │
│                                                              │
│  ┌──────────────┐  ┌───────────┐  ┌───────────────────┐     │
│  │  KEY VAULT   │  │    LOG    │  │  ACTION GROUP      │     │
│  │  (Secretos)  │  │ ANALYTICS │  │  (Email alerts)    │     │
│  └──────────────┘  └───────────┘  └───────────────────┘     │
└──────────────────────────────────────────────────────────────┘
       ↑ Fuente                          ↓ Consumo
┌──────────────┐               ┌────────────────────┐
│  Azure SQL   │               │  Power BI / Looker │
│  Database    │               │  (dim + fact Gold)  │
│ (Transacc.)  │               └────────────────────┘
└──────────────┘
```

---

## Estructura del Repositorio

```
finbank-data-pipeline/
├── README.md                    # Este documento
├── CHANGELOG.md                 # Historial de cambios
├── config/
│   └── pipeline_config.yaml     # Configuración centralizada
├── data-generation/
│   ├── generate_data.py         # Generador de datos sintéticos
│   ├── load_to_db.py            # Carga a SQL Database
│   └── output/                  # Datos generados (csv + parquet)
├── infra/
│   ├── main.tf                  # Recursos Azure
│   ├── variables.tf             # Variables parametrizadas
│   ├── outputs.tf               # Outputs del módulo
│   └── environments/
│       ├── dev.tfvars           # Variables de dev
│       └── prod.tfvars          # Variables de prod
├── pipelines/
│   ├── bronze/
│   │   └── ingest_bronze.py     # Ingesta Bronze
│   ├── silver/
│   │   └── transform_silver.py  # Limpieza y conformación
│   ├── gold/
│   │   └── transform_gold.py    # Modelo dimensional
│   └── quality/
│       └── quality_checks.py    # 5 checks automatizados
├── orchestration/
│   ├── create_workflow.py       # Definición del DAG
│   └── send_notification.py     # Script de notificaciones
├── docs/
│   ├── er_diagram.md            # Diagrama ER
│   ├── data_catalog.md          # Catálogo de datos
│   ├── lineage.md               # Linaje de datos
│   └── architecture_diagram.png # Diagrama de arquitectura
└── tests/
    └── test_quality.py          # Tests unitarios
```

---

## Guía de Despliegue

### Pre-requisitos
- Azure CLI instalado y autenticado (`az login`)
- Terraform >= 1.5.0
- Python >= 3.10 con `pandas`, `numpy`, `pyyaml`, `pyarrow`
- Databricks CLI configurado

### Paso 1: Infraestructura
```bash
cd infra/
terraform init
terraform plan -var-file="environments/dev.tfvars" -var="sql_admin_password=$SQL_PASS"
terraform apply -var-file="environments/dev.tfvars" -var="sql_admin_password=$SQL_PASS"
```

### Paso 2: Generar datos sintéticos
```bash
cd data-generation/
pip install pandas numpy pyyaml pyarrow
python generate_data.py --config ../config/pipeline_config.yaml --output ./output
```

### Paso 3: Cargar datos a SQL Database
```bash
python load_to_db.py --config ../config/pipeline_config.yaml --data-dir ./output/csv
```

### Paso 4: Desplegar notebooks en Databricks
```bash
databricks repos create --url <GIT_REPO_URL> --path /Repos/finbank-pipeline
```

### Paso 5: Crear y ejecutar el Workflow
```bash
cd orchestration/
python create_workflow.py
databricks jobs create --json @workflow_definition.json
```

---

## Supuestos Documentados

1. **Tasa de cambio COP/USD**: Se usa una tasa fija de 0.000245 (referencial). En producción se consumiría una API de Banrep.
2. **Horario hábil**: Se define como 07:00 a 18:00 horas. Ajustable via config.
3. **CLTV simplificado**: Se calcula como intereses estimados (1.5% del volumen transaccional) + comisiones cobradas. En producción se usaría un modelo financiero más sofisticado.
4. **Provisión regulatoria**: Se usan porcentajes simplificados basados en la Circular Básica Contable de la SFC. Los valores reales dependen del modelo interno del banco.
5. **Anomalías intencionales**: 500 duplicados, 200 fechas futuras, 250 registros inconsistentes — todos documentados y manejados explícitamente por el pipeline.

---

## Contacto
**Candidato**: Miguel Ángel  
**Fecha de entrega**: [FECHA]
