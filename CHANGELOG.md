# CHANGELOG

Historial de cambios del proyecto FinBank End-to-End Data Pipeline.

## [1.0.0] — 2025-03-29

### Fase 1 — Generación de Datos
- **Autor**: Miguel Ángel
- Implementado generador de datos sintéticos con semilla fija (seed=42)
- 6 tablas generadas con distribuciones realistas (log-normal para montos, normal para edades)
- Integridad referencial garantizada entre tablas
- ~5% de valores nulos inyectados en campos no críticos
- 3 anomalías documentadas: 500 duplicados, 200 fechas futuras, 250 registros inconsistentes
- Exportación en CSV y Parquet (ingesta heterogénea)
- Archivo de configuración centralizado en YAML

### Fase 2 — Infraestructura como Código
- **Autor**: Miguel Ángel
- Terraform con backend remoto en Azure Storage
- Resource Group, ADLS Gen2 (6 contenedores), Databricks Workspace, ADF, Key Vault, Log Analytics, SQL Database
- Soporte para entornos dev/prod via tfvars
- RBAC implementado: Data Engineer, Analyst, Admin
- Diagnostic settings habilitados para auditoría de accesos

### Fase 3 — Pipeline Medallion
- **Autor**: Miguel Ángel
- **Bronze**: Ingesta incremental con MERGE, columnas de auditoría, particionamiento año/mes/día
- **Silver**: Deduplicación, enmascaramiento SHA-256 de PII, validación referencial, flag ind_sospechoso (3σ), reporte de calidad
- **Gold**: dim_clientes, dim_productos, dim_geografia, dim_canal, fact_transacciones, fact_cartera, fact_rentabilidad_cliente, kpi_cartera_diaria
- 5 checks de calidad automatizados (volumen, completeness, referential integrity, date ranges, uniqueness)
- Tabla de errores del pipeline con registros rechazados categorizados

### Fase 4 — Orquestación
- **Autor**: Miguel Ángel
- Databricks Workflow con 5 tareas encadenadas (Bronze → Silver → Gold → Quality → Notification)
- Programación diaria 02:00 COT
- Retries con backoff exponencial (3 intentos)
- Alertas por email ante fallo y reporte diario de éxito
- Timeouts por tarea proporcionados al volumen

### Fase 5 — Gobierno y Seguridad
- **Autor**: Miguel Ángel
- 3 roles RBAC: Data Engineer (R/W all), Analyst (RO Gold), Admin (full)
- Enmascaramiento SHA-256 de PII en Silver (num_doc, nombres, id_dispositivo)
- Catálogo de datos en Markdown (Silver + Gold)
- Linaje documentado para 4 campos calculados
- 3 tipos de alertas: fallo, reporte diario, anomalía de volumen
- Log Analytics con Diagnostic Settings para auditoría de accesos
