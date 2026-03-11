# CLIENTES
CREATE OR REPLACE EXTERNAL TABLE {project_id}.clickup.clientes
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/clientes/clientes.csv']);

-- GOLD
# CLIENTES
CREATE OR REPLACE TABLE `{project_id}.vendas.clickup_clientes_gold`
AS
SELECT *
FROM `{project_id}.clickup.clientes`;
