# CUSTOMERS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.eduzz.customers
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/customers/customers.csv']);

# SUBSCRIPTIONS_CREATION
CREATE OR REPLACE EXTERNAL TABLE {project_id}.eduzz.subscriptions_creation
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/subscriptions_creation/subscriptions_creation.csv']);

# SUBSCRIPTIONS_UPDATE
CREATE OR REPLACE EXTERNAL TABLE {project_id}.eduzz.subscriptions_update
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/subscriptions_update/subscriptions_update.csv']);

# SALES
CREATE OR REPLACE EXTERNAL TABLE {project_id}.eduzz.sales
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/sales/sales.csv']);

-- GOLD
# CUSTOMERS
CREATE OR REPLACE TABLE `{project_id}.vendas.eduzz_customers_gold`
AS
SELECT *
FROM `{project_id}.eduzz.customers`;

# SUBSCRIPTIONS_CREATION
CREATE OR REPLACE TABLE `{project_id}.vendas.eduzz_subscriptions_creation_gold`
AS
SELECT *
FROM `{project_id}.eduzz.subscriptions_creation`;

# SUBSCRIPTIONS_UPDATE
CREATE OR REPLACE TABLE `{project_id}.vendas.eduzz_subscriptions_update_gold`
AS
SELECT *
FROM `{project_id}.eduzz.subscriptions_update`;

# SALES
CREATE OR REPLACE TABLE `{project_id}.vendas.eduzz_sales_gold`
AS
SELECT *
FROM `{project_id}.eduzz.sales`;