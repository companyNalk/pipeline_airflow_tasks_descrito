# ACTIVITIES
CREATE OR REPLACE EXTERNAL TABLE {project_id}.piperun.activities
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/activities/activities.csv']);

# COMPANIES
CREATE OR REPLACE EXTERNAL TABLE {project_id}.piperun.companies
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/companies/companies.csv']);

# DEALS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.piperun.deals
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/deals/deals.csv']);

# LOST_REASONS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.piperun.lost_reasons
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/lost_reasons/lost_reasons.csv']);

# ORIGINS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.piperun.origins
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/origins/origins.csv']);

-- GOLD
# ACTIVITIES
CREATE OR REPLACE TABLE `{project_id}.vendas.piperun_activities_gold`
AS
SELECT *
FROM `{project_id}.piperun.activities`;

# COMPANIES
CREATE OR REPLACE TABLE `{project_id}.vendas.piperun_companies_gold`
AS
SELECT *
FROM `{project_id}.piperun.companies`;

# DEALS
CREATE OR REPLACE TABLE `{project_id}.vendas.piperun_deals_gold`
AS
SELECT *
FROM `{project_id}.piperun.deals`;

# LOST_REASONS
CREATE OR REPLACE TABLE `{project_id}.vendas.piperun_lost_reasons_gold`
AS
SELECT *
FROM `{project_id}.piperun.lost_reasons`;

# ORIGINS
CREATE OR REPLACE TABLE `{project_id}.vendas.piperun_origins_gold`
AS
SELECT *
FROM `{project_id}.piperun.origins`;