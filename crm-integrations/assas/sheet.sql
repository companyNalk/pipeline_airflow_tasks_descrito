# PAYMENTS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.assas.payments
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/payments/payments.csv']);

# CUSTOMERS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.assas.customers
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/customers/customers.csv']);

# SUBSCRIPTIONS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.assas.subscriptions
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/subscriptions/subscriptions.csv']);

# SUBSCRIPTIONS_ID_PAYMENTS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.assas.subscriptions_id_payments
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/subscriptions_id_payments/subscriptions_id_payments.csv']);

-- GOLD
# PAYMENTS
CREATE OR REPLACE TABLE `{project_id}.vendas.assas_payments_gold`
AS
SELECT *
FROM `{project_id}.assas.payments`;

# CUSTOMERS
CREATE OR REPLACE TABLE `{project_id}.vendas.assas_customers_gold`
AS
SELECT *
FROM `{project_id}.assas.customers`;

# SUBSCRIPTIONS
CREATE OR REPLACE TABLE `{project_id}.vendas.assas_subscriptions_gold`
AS
SELECT *
FROM `{project_id}.assas.subscriptions`;

# SUBSCRIPTIONS_ID_PAYMENTS
CREATE OR REPLACE TABLE `{project_id}.vendas.assas_subscriptions_id_payments_gold`
AS
SELECT *
FROM `{project_id}.assas.subscriptions_id_payments`;