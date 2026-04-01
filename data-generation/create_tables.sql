-- =============================================================================
-- DDL — Azure SQL Database: finbank_transactional
-- Escenario A: Banca y Servicios Financieros — FinBank S.A.
-- =============================================================================
-- Ejecutar después del terraform apply para crear las tablas fuente.
-- Uso: sqlcmd -S sql-finbank-source-dev.database.windows.net -d finbank_transactional \
--       -U finbank_admin -P <password> -i create_tables.sql
-- =============================================================================

-- Tabla de clientes
CREATE TABLE dbo.TB_CLIENTES_CORE (
    id_cli          INT             PRIMARY KEY,
    nomb_cli        NVARCHAR(100)   NOT NULL,
    apell_cli       NVARCHAR(150)   NOT NULL,
    tip_doc         VARCHAR(5)      NOT NULL CHECK (tip_doc IN ('CC','CE','PA','TI')),
    num_doc         VARCHAR(20)     NOT NULL,
    fec_nac         DATE            NOT NULL,
    fec_alta        DATE            NOT NULL,
    cod_segmento    VARCHAR(5)      NOT NULL CHECK (cod_segmento IN ('01','02','03','04')),
    score_buro      INT             NULL CHECK (score_buro BETWEEN 150 AND 950 OR score_buro IS NULL),
    ciudad_res      NVARCHAR(100)   NULL,
    depto_res       NVARCHAR(100)   NULL,
    estado_cli      VARCHAR(20)     NOT NULL DEFAULT 'ACTIVO',
    canal_adquis    VARCHAR(20)     NULL,
    CONSTRAINT UQ_num_doc UNIQUE (tip_doc, num_doc)
);

CREATE INDEX IX_clientes_segmento ON dbo.TB_CLIENTES_CORE (cod_segmento);
CREATE INDEX IX_clientes_estado ON dbo.TB_CLIENTES_CORE (estado_cli);
CREATE INDEX IX_clientes_ciudad ON dbo.TB_CLIENTES_CORE (ciudad_res);

-- Catálogo de productos
CREATE TABLE dbo.TB_PRODUCTOS_CAT (
    cod_prod        VARCHAR(10)     PRIMARY KEY,
    desc_prod       NVARCHAR(200)   NOT NULL,
    tip_prod        VARCHAR(50)     NOT NULL,
    tasa_ea         DECIMAL(8,4)    NOT NULL DEFAULT 0,
    plazo_max_meses INT             NOT NULL DEFAULT 0,
    cuota_min       DECIMAL(15,2)   NOT NULL DEFAULT 0,
    comision_admin  DECIMAL(15,2)   NOT NULL DEFAULT 0,
    estado_prod     VARCHAR(20)     NOT NULL DEFAULT 'ACTIVO'
);

-- Red de sucursales
CREATE TABLE dbo.TB_SUCURSALES_RED (
    cod_suc         VARCHAR(10)     PRIMARY KEY,
    nom_suc         NVARCHAR(200)   NOT NULL,
    tip_punto       VARCHAR(30)     NOT NULL,
    ciudad          NVARCHAR(100)   NOT NULL,
    depto           NVARCHAR(100)   NOT NULL,
    latitud         DECIMAL(10,6)   NULL,
    longitud        DECIMAL(10,6)   NULL,
    activo          BIT             NOT NULL DEFAULT 1
);

CREATE INDEX IX_sucursales_ciudad ON dbo.TB_SUCURSALES_RED (ciudad);

-- Movimientos financieros (tabla de mayor volumen)
CREATE TABLE dbo.TB_MOV_FINANCIEROS (
    id_mov          BIGINT          PRIMARY KEY,
    id_cli          INT             NOT NULL,
    cod_prod        VARCHAR(10)     NOT NULL,
    num_cuenta      VARCHAR(30)     NOT NULL,
    fec_mov         DATE            NOT NULL,
    hra_mov         VARCHAR(10)     NOT NULL,
    vr_mov          DECIMAL(18,2)   NOT NULL CHECK (vr_mov > 0),
    tip_mov         VARCHAR(30)     NOT NULL,
    cod_canal       VARCHAR(20)     NOT NULL,
    cod_ciudad      NVARCHAR(100)   NULL,
    cod_estado_mov  VARCHAR(20)     NOT NULL DEFAULT 'APROBADO',
    id_dispositivo  VARCHAR(20)     NULL,
    CONSTRAINT FK_mov_cliente FOREIGN KEY (id_cli) REFERENCES dbo.TB_CLIENTES_CORE(id_cli),
    CONSTRAINT FK_mov_producto FOREIGN KEY (cod_prod) REFERENCES dbo.TB_PRODUCTOS_CAT(cod_prod)
);

CREATE INDEX IX_mov_fecha ON dbo.TB_MOV_FINANCIEROS (fec_mov);
CREATE INDEX IX_mov_cliente ON dbo.TB_MOV_FINANCIEROS (id_cli);
CREATE INDEX IX_mov_producto ON dbo.TB_MOV_FINANCIEROS (cod_prod);
CREATE INDEX IX_mov_canal ON dbo.TB_MOV_FINANCIEROS (cod_canal);

-- Obligaciones (cartera de crédito)
CREATE TABLE dbo.TB_OBLIGACIONES (
    id_oblig            INT             PRIMARY KEY,
    id_cli              INT             NOT NULL,
    cod_prod            VARCHAR(10)     NOT NULL,
    vr_aprobado         DECIMAL(18,2)   NOT NULL,
    vr_desembolsado     DECIMAL(18,2)   NOT NULL,
    sdo_capital         DECIMAL(18,2)   NOT NULL,
    vr_cuota            DECIMAL(18,2)   NULL,
    fec_desembolso      DATE            NOT NULL,
    fec_venc            DATE            NOT NULL,
    dias_mora_act       INT             NOT NULL DEFAULT 0,
    num_cuotas_pend     INT             NULL,
    calif_riesgo        CHAR(1)         NOT NULL CHECK (calif_riesgo IN ('A','B','C','D','E')),
    CONSTRAINT FK_oblig_cliente FOREIGN KEY (id_cli) REFERENCES dbo.TB_CLIENTES_CORE(id_cli),
    CONSTRAINT FK_oblig_producto FOREIGN KEY (cod_prod) REFERENCES dbo.TB_PRODUCTOS_CAT(cod_prod)
);

CREATE INDEX IX_oblig_cliente ON dbo.TB_OBLIGACIONES (id_cli);
CREATE INDEX IX_oblig_mora ON dbo.TB_OBLIGACIONES (dias_mora_act);
CREATE INDEX IX_oblig_calif ON dbo.TB_OBLIGACIONES (calif_riesgo);

-- Log de comisiones
CREATE TABLE dbo.TB_COMISIONES_LOG (
    id_comision     INT             PRIMARY KEY,
    id_cli          INT             NOT NULL,
    cod_prod        VARCHAR(10)     NOT NULL,
    fec_cobro       DATE            NOT NULL,
    vr_comision     DECIMAL(15,2)   NOT NULL,
    tip_comision    VARCHAR(30)     NULL,
    estado_cobro    VARCHAR(20)     NOT NULL DEFAULT 'COBRADO',
    CONSTRAINT FK_com_cliente FOREIGN KEY (id_cli) REFERENCES dbo.TB_CLIENTES_CORE(id_cli),
    CONSTRAINT FK_com_producto FOREIGN KEY (cod_prod) REFERENCES dbo.TB_PRODUCTOS_CAT(cod_prod)
);

CREATE INDEX IX_com_cliente ON dbo.TB_COMISIONES_LOG (id_cli);
CREATE INDEX IX_com_fecha ON dbo.TB_COMISIONES_LOG (fec_cobro);
CREATE INDEX IX_com_estado ON dbo.TB_COMISIONES_LOG (estado_cobro);

-- Verificación

SELECT 
    t.name AS tabla,
    p.rows AS registros
FROM sys.tables t
INNER JOIN sys.partitions p ON t.object_id = p.object_id AND p.index_id IN (0,1)
ORDER BY t.name;
GO
