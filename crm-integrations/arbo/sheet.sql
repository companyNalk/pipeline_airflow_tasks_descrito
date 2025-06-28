# LEADS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.arbo.leads
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/leads/leads.csv']);

# IMOVEIS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.arbo.imoveis
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/imoveis/imoveis.csv']);

-- GOLD
# LEADS
CREATE OR REPLACE TABLE `{project_id}.vendas.arbo_leads_gold`
AS
SELECT *
FROM `{project_id}.arbo.leads`;

# IMOVEIS
CREATE OR REPLACE TABLE `{project_id}.vendas.arbo_imoveis_gold`
AS
SELECT *
FROM `{project_id}.arbo.imoveis`;