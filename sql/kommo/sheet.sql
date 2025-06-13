# LEADS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.kommo.leads
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/leads/leads.csv']);

# PIPELINES
CREATE OR REPLACE EXTERNAL TABLE {project_id}.kommo.pipelines
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/pipelines/pipelines.csv']);

# USERS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.kommo.users
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/users/users_list.csv']);

# UNSORTED_LEADS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.kommo.unsorted_leads
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/unsorted_leads/unsorted_leads.csv']);

-- GOLD
# LEADS
CREATE OR REPLACE TABLE `{project_id}.vendas.kommo_leads_gold`
AS
SELECT *
FROM `{project_id}.kommo.leads`;

# PIPELINES
CREATE OR REPLACE TABLE `{project_id}.vendas.kommo_pipelines_gold`
AS
SELECT *
FROM `{project_id}.kommo.pipelines`;

# USERS
CREATE OR REPLACE TABLE `{project_id}.vendas.kommo_users_gold`
AS
SELECT *
FROM `{project_id}.kommo.users`;

# UNSORTED_LEADS
CREATE OR REPLACE TABLE `{project_id}.vendas.kommo_unsorted_leads_gold`
AS
SELECT *
FROM `{project_id}.kommo.unsorted_leads`;