# ACTIVITIES
CREATE OR REPLACE EXTERNAL TABLE {project_id}.pipedrive.activities
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/activities/activities.csv']);

# DEALS_DADOS_FINAIS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.pipedrive.deals_dados_finais
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/deals_dados_finais/enriched_deals.csv']);

# LEADS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.pipedrive.leads
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/leads/leads.csv']);

# PERSONS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.pipedrive.persons
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/persons/persons.csv']);

# PIPELINES
CREATE OR REPLACE EXTERNAL TABLE {project_id}.pipedrive.pipelines
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/pipelines/pipelines.csv']);

# PRODUCTS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.pipedrive.products
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/products/products.csv']);

# STAGES
CREATE OR REPLACE EXTERNAL TABLE {project_id}.pipedrive.stages
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/stages/stages.csv']);

-- GOLD
# ACTIVITIES
CREATE OR REPLACE TABLE `{project_id}.vendas.pipedrive_activities_gold`
AS
SELECT *
FROM `{project_id}.pipedrive.activities`;

# DEALS_DADOS_FINAIS
CREATE OR REPLACE TABLE `{project_id}.vendas.pipedrive_deals_dados_finais_gold`
AS
SELECT *
FROM `{project_id}.pipedrive.deals_dados_finais`;

# LEADS
CREATE OR REPLACE TABLE `{project_id}.vendas.pipedrive_leads_gold`
AS
SELECT *
FROM `{project_id}.pipedrive.leads`;

# PERSONS
CREATE OR REPLACE TABLE `{project_id}.vendas.pipedrive_persons_gold`
AS
SELECT *
FROM `{project_id}.pipedrive.persons`;

# PIPELINES
CREATE OR REPLACE TABLE `{project_id}.vendas.pipedrive_pipelines_gold`
AS
SELECT *
FROM `{project_id}.pipedrive.pipelines`;

# PRODUCTS
CREATE OR REPLACE TABLE `{project_id}.vendas.pipedrive_products_gold`
AS
SELECT *
FROM `{project_id}.pipedrive.products`;

# STAGES
CREATE OR REPLACE TABLE `{project_id}.vendas.pipedrive_stages_gold`
AS
SELECT *
FROM `{project_id}.pipedrive.stages`;