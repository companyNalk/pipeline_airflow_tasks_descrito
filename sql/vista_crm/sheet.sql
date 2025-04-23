# PIPELINES
CREATE OR REPLACE EXTERNAL TABLE {project_id}.vista_crm.pipelines
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/pipelines/pipelines.csv']);

# ETAPAS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.vista_crm.etapas
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/etapas/etapas.csv']);

# NEGOCIOS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.vista_crm.negocios
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/negocios/negocios.csv']);

# CAMPOS_CLIENTES
CREATE OR REPLACE EXTERNAL TABLE {project_id}.vista_crm.campos_clientes
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/campos_clientes/campos_clientes.csv']);

# CLIENTES
CREATE OR REPLACE EXTERNAL TABLE {project_id}.vista_crm.clientes
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/clientes/clientes.csv']);

# CAMPOS_IMOVEIS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.vista_crm.campos_imoveis
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/campos_imoveis/campos_imoveis.csv']);

# IMOVEIS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.vista_crm.imoveis
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/imoveis/imoveis.csv']);

-- GOLD
# PIPELINES
CREATE OR REPLACE TABLE `{project_id}.vendas.vista_crm_pipelines_gold`
AS
SELECT *
FROM `{project_id}.vista_crm.pipelines`;

# ETAPAS
CREATE OR REPLACE TABLE `{project_id}.vendas.vista_crm_etapas_gold`
AS
SELECT *
FROM `{project_id}.vista_crm.etapas`;

# NEGOCIOS
CREATE OR REPLACE TABLE `{project_id}.vendas.vista_crm_negocios_gold`
AS
SELECT *
FROM `{project_id}.vista_crm.negocios`;

# CAMPOS_CLIENTES
CREATE OR REPLACE TABLE `{project_id}.vendas.vista_crm_campos_clientes_gold`
AS
SELECT *
FROM `{project_id}.vista_crm.campos_clientes`;

# CLIENTES
CREATE OR REPLACE TABLE `{project_id}.vendas.vista_crm_clientes_gold`
AS
SELECT *
FROM `{project_id}.vista_crm.clientes`;

# CAMPOS_IMOVEIS
CREATE OR REPLACE TABLE `{project_id}.vendas.vista_crm_campos_imoveis_gold`
AS
SELECT *
FROM `{project_id}.vista_crm.campos_imoveis`;

# IMOVEIS
CREATE OR REPLACE TABLE `{project_id}.vendas.vista_crm_imoveis_gold`
AS
SELECT *
FROM `{project_id}.vista_crm.imoveis`;