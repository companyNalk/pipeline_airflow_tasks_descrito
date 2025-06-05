# CLIENTES
CREATE OR REPLACE EXTERNAL TABLE {project_id}.vista_crm.clientes
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/tabela_clientes/clientes.csv']);

# CORRETORES
CREATE OR REPLACE EXTERNAL TABLE {project_id}.vista_crm.corretores
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/tabela_corretores/corretores.csv']);

# FUNIL_LOCACAO
CREATE OR REPLACE EXTERNAL TABLE {project_id}.vista_crm.funil_locacao
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/funil_locacao/funil_locacao.csv']);

# FUNIL_VENDAS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.vista_crm.funil_vendas
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/funil_vendas/funil_vendas.csv']);

# IMOVEIS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.vista_crm.imoveis
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/tabela_imoveis/imoveis.csv']);

-- GOLD
# CLIENTES
CREATE OR REPLACE TABLE `{project_id}.vendas.vista_crm_clientes_gold`
AS
SELECT *
FROM `{project_id}.vista_crm.clientes`;

# CORRETORES
CREATE OR REPLACE TABLE `{project_id}.vendas.vista_crm_corretores_gold`
AS
SELECT *
FROM `{project_id}.vista_crm.corretores`;

# FUNIL_LOCACAO
CREATE OR REPLACE TABLE `{project_id}.vendas.vista_crm_funil_locacao_gold`
AS
SELECT *
FROM `{project_id}.vista_crm.funil_locacao`;

# FUNIL_VENDAS
CREATE OR REPLACE TABLE `{project_id}.vendas.vista_crm_funil_vendas_gold`
AS
SELECT *
FROM `{project_id}.vista_crm.funil_vendas`;

# IMOVEIS
CREATE OR REPLACE TABLE `{project_id}.vendas.vista_crm_imoveis_gold`
AS
SELECT *
FROM `{project_id}.vista_crm.imoveis`;