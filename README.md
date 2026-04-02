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

### ¿Por qué West US 2?
East US 2 no estaba disponible para la suscripción free trial al momento del aprovisionamiento. West US 2 ofrece la misma disponibilidad de servicios (Databricks, ADLS Gen2, Data Factory, Key Vault, Log Analytics) con precios equivalentes.

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
│                 DATABRICKS WORKFLOWS                         │
│           (Trigger diario 02:00 COT / manual)                │
│                         │                                    │
│    ┌────────────────────┼────────────────────┐               │
│    │          PIPELINE MEDALLION             │               │
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
│    │  │   ADLS Gen2                      │  │               │
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
│
├── README.md                          # Este documento
├── CHANGELOG.md                       # Historial de cambios
├── environment.yml                    # Entorno Conda (Python 3.11 + dependencias)
├── requirements.txt                   # Dependencias pip alternativas
├── .gitignore                         # Excluye secretos, .tfstate, datos generados
│
├── config/
│   └── pipeline_config.yaml           # Configuración centralizada (seed, volúmenes,
│                                      #   reglas de negocio, umbrales, tasas FX)
│
├── data-generation/
│   ├── generate_data.py               # Generador de datos sintéticos (seed=42)
│   ├── load_to_sql.py                 # Carga a Azure SQL (credenciales via env vars)
│   ├── create_tables.sql              # DDL para Azure SQL Server con constraints
│   └── output/                        # CSVs y Parquets generados (no versionado)
│
├── infra/
│   ├── main.tf                        # Recursos Azure (ADLS Gen2, Databricks, ADF,
│   │                                  #   Key Vault, Log Analytics, Action Group)
│   ├── variables.tf                   # Variables parametrizadas (sin credenciales)
│   ├── outputs.tf                     # Outputs para consumo del pipeline
│   ├── README.md                      # Instrucciones de despliegue Terraform
│   └── environments/
│       ├── dev.tfvars                 # Variables de dev (westus2)
│       └── prod.tfvars                # Variables de prod
│
├── pipelines/
│   ├── bronze/
│   │   └── ingest_bronze.py           # Ingesta SQL → Delta Lake con auditoría
│   ├── silver/
│   │   └── transform_silver.py        # Limpieza, dedup, PII masking, ind_sospechoso
│   ├── gold/
│   │   └── transform_gold.py          # Modelo dimensional + reglas de negocio
│   └── quality/
│       └── quality_checks.py          # 5 verificaciones automatizadas
│
├── orchestration/
│   ├── create_workflow.py             # Generador del JSON de Databricks Workflow
│   ├── workflow_definition.json       # Definición del DAG exportada
│   ├── setup_databricks.py            # Setup inicial del workspace
│   └── send_notification.py           # Notificación éxito/fallo + anomalía volumen
│
├── docs/
│   ├── er_diagram.md                  # Diagrama ER (Mermaid) fuente + Gold
│   ├── data_catalog.md                # Catálogo de datos Silver y Gold
│   └── lineage.md                     # Linaje de 4 campos calculados en Gold
│
└── .github/
    └── workflows/
        └── ci-cd.yml                  # GitHub Actions: lint, validate, security scan
```

---

## Guía de Despliegue

### Pre-requisitos

- **Windows 10/11** con Anaconda instalado
- **Azure CLI**: https://aka.ms/installazurecliwindows
- **Terraform**: https://developer.hashicorp.com/terraform/install (extraer a `C:\terraform\` y agregar al PATH)
- **Git**: https://git-scm.com/download/win
- **ODBC Driver 18 for SQL Server**: https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server

### Paso 1: Crear el entorno Python

```bash
conda env create -f environment.yml
conda activate finbank
```

### Paso 2: Generar datos sintéticos

```bash
cd data-generation/
python generate_data.py --config ../config/pipeline_config.yaml --output ./output
```

Genera 620,750 registros en 6 tablas con distribuciones realistas, ~5% de nulos, y 3 anomalías intencionales documentadas.

### Paso 3: Crear Azure SQL Database (via portal)

- **Portal** → SQL databases → Create
- Server: crear nuevo con SQL authentication
- Tier: **Basic** (5 DTUs)
- Region: **West US 2**
- Backup redundancy: **Locally-redundant**
- Networking: Public endpoint + Allow Azure services

Ejecutar `create_tables.sql` desde el Query Editor del portal.

### Paso 4: Cargar datos a Azure SQL

```powershell
$env:SQL_SERVER = "tu-servidor.database.windows.net"
$env:SQL_DATABASE = "finbank_transactional"
$env:SQL_USER = "finbank_admin"
$env:SQL_PASSWORD = "tu_password"
python load_to_sql.py --data-dir ./output/csv
```

### Paso 5: Infraestructura con Terraform

```powershell
cd infra/
az login
$env:TF_VAR_sql_admin_password = "tu_password"

terraform init `
  -backend-config="resource_group_name=rg-terraform-state" `
  -backend-config="storage_account_name=stterraformfinbankXX" `
  -backend-config="container_name=tfstate" `
  -backend-config="key=finbank-pipeline.terraform.tfstate"

terraform plan -var-file="environments\dev.tfvars"
terraform apply -var-file="environments\dev.tfvars"
```

**Nota**: El SQL Database se creó manualmente (Paso 3), por lo que sus bloques están comentados en `main.tf`. En producción, todos los recursos se gestionarían exclusivamente via Terraform.

### Paso 6: Configurar Databricks

1. Portal → Databricks Workspace → **Launch Workspace**
2. Crear Secret Scope (`finbank-secrets`) vinculado a Key Vault con Manage Principal = **All Users**
3. Importar notebooks desde Git (Repos → Add Repo)
4. Configurar acceso a ADLS Gen2 con Storage Account Key

### Paso 7: Ejecutar pipeline

Ejecutar notebooks en orden: Bronze → Silver → Gold → Quality Checks → Notification. O crear Databricks Workflow para ejecución automática diaria a las 02:00 COT.

---

## Datos Generados

| Tabla | Registros | Descripción |
|-------|-----------|-------------|
| TB_CLIENTES_CORE | 10,000 | Clientes del banco (5 países LATAM) |
| TB_PRODUCTOS_CAT | 50 | Catálogo de productos financieros |
| TB_SUCURSALES_RED | 200 | Red de sucursales y corresponsales |
| TB_MOV_FINANCIEROS | 500,500 | Transacciones (incluye 500 duplicados + 200 fechas futuras) |
| TB_OBLIGACIONES | 30,000 | Cartera de crédito (incluye 150 mora negativa + 100 sdo inconsistente) |
| TB_COMISIONES_LOG | 80,000 | Log de comisiones cobradas |
| **TOTAL** | **620,750** | |

### Anomalías intencionales

1. **500 transacciones duplicadas** en TB_MOV_FINANCIEROS — mismos datos con id_mov diferente. Detectadas en Silver via `dropDuplicates` excluyendo PK.
2. **200 fechas futuras (2026+)** en TB_MOV_FINANCIEROS — rechazadas en Silver → tabla de errores.
3. **250 registros inconsistentes** en TB_OBLIGACIONES — 150 mora negativa + 100 sdo > aprobado. Rechazados en Silver con motivo documentado.

---

## Pipeline Medallion

### Bronze
- Lectura JDBC desde Azure SQL Database
- Delta Lake con particionamiento año/mes/día
- 3 columnas de auditoría: `_ingestion_timestamp`, `_source_system`, `_batch_id`
- Log de ejecución en `/logs/bronze_ingestion_log`

### Silver
- Eliminación de duplicados (excluyendo PK)
- Enmascaramiento SHA-256: `num_doc`, `nomb_cli`, `apell_cli`, `id_dispositivo`
- Validación de integridad referencial (huérfanos → tabla errores con `mergeSchema`)
- Flag `ind_sospechoso` (3σ ventana 30 días por cliente)
- Reporte de calidad por ejecución

### Gold
- **dim_clientes**: edad, grupo_edad, segmento legible, antigüedad
- **dim_productos**: tasa mensual equivalente, familia producto
- **dim_geografia**: ciudad + departamento deduplicado
- **dim_canal**: tipo punto de atención y canal digital
- **fact_transacciones**: monto USD, flag horario, ind_sospechoso
- **fact_cartera**: bucket_mora, clasificación regulatoria A-E, provisión estimada
- **fact_rentabilidad_cliente**: CLTV 12 meses (calculado desde fecha máxima de los datos)
- **kpi_cartera_diaria**: agregación por fecha, producto, segmento, ciudad

### Quality Checks
1. Volumen: row_count > 0 en todas las tablas Gold
2. Completeness: no PKs nulas
3. Integridad referencial: fact → dim
4. Validez temporal: no fechas futuras
5. Unicidad: no PKs duplicadas

---

## Orquestación

DAG de 5 tareas secuenciales en Databricks Workflows:

```
Schedule (02:00 COT) → Bronze → Silver → Gold → Quality → Notification
```

- Retries: 3 con backoff exponencial
- Timeouts: 30 / 45 / 60 / 15 / 5 minutos
- Alertas por email en fallo y éxito
- Job Cluster: Standard_DS3_v2, Single Node, auto-terminate 20min

---

## Gobierno y Seguridad

### Roles RBAC

| Rol | Alcance | Permisos |
|-----|---------|----------|
| Data Engineer | Storage Account | Storage Blob Data Contributor |
| Analyst | Contenedor `/gold` únicamente | Storage Blob Data Reader + Reader en RG |
| Admin | Resource Group | Contributor |

### Privacidad
- PII enmascarado con SHA-256 desde Silver
- Analyst sin acceso a Bronze/Silver (solo Gold con datos ya enmascarados)
- Credenciales via variables de entorno y Key Vault (nunca en código)

### Auditoría
- Diagnostic Settings en Storage Account (Read/Write/Delete → Log Analytics)
- Logs de ejecución por capa en Delta Lake (`/logs/`)
- Tabla de errores centralizada (`/errors/silver_rejected`)

---

## Supuestos Documentados

1. **Región West US 2** por indisponibilidad de East US 2 en free trial
2. **SQL Database creado por portal** (bloques Terraform comentados y documentados)
3. **Secret Scope con "All Users"** (SKU Standard no soporta "Creator")
4. **Storage Key en notebooks** (en producción: Managed Identity o Unity Catalog)
5. **Tasa COP/USD fija** (0.000245) — en producción: API de Banrep
6. **CLTV desde fecha máxima de datos** (no `current_date`) para compatibilidad con datos sintéticos históricos
7. **Provisión regulatoria simplificada** basada en Circular Básica Contable de la SFC

---

## Elementos Diferenciadores

- Delta Lake (ACID, time travel, MERGE)
- Configuración YAML centralizada y parametrizable
- 3 anomalías documentadas y manejadas explícitamente
- Reporte de calidad automático por ejecución
- Alerta de anomalía de volumen (±30% vs promedio)
- Linaje documentado para 4 campos calculados
- CI/CD con GitHub Actions
- Entorno Conda reproducible (`environment.yml`)
- Multi-entorno dev/prod en Terraform
- Diagnostic Settings para auditoría de accesos

---

## Contacto

**Candidato**: Miguel Ángel Palomino  
**Repositorio**: https://github.com/Mikepa22/finbank-data-pipeline  
**Fecha de entrega**: Abril 2026
