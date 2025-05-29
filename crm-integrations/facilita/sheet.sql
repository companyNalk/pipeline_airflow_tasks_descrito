# NEGOCIOS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.facilita.negocios
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/negocios/negocios.csv']);

# LEADS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.facilita.leads
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/leads/leads.csv']);

-- GOLD
# NEGOCIOS
CREATE OR REPLACE TABLE `{project_id}.vendas.facilita_negocios_gold`
AS
SELECT *
FROM `{project_id}.facilita.negocios`;

# LEADS
CREATE OR REPLACE TABLE `{project_id}.vendas.facilita_leads_gold`
AS
SELECT *
FROM `{project_id}.facilita.leads`;