# PAYMENTS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.asaas.payments
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/payments/payments.csv']);

# CUSTOMERS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.asaas.customers
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/customers/customers.csv']);

# SUBSCRIPTIONS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.asaas.subscriptions
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/subscriptions/subscriptions.csv']);

# SUBSCRIPTIONS_ID_PAYMENTS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.asaas.subscriptions_id_payments
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/subscriptions_id_payments/subscriptions_id_payments.csv']);

-- GOLD
# PAYMENTS
CREATE OR REPLACE TABLE `{project_id}.vendas.asaas_payments_gold`
AS
SELECT *
FROM `{project_id}.asaas.payments`;

# CUSTOMERS
CREATE OR REPLACE TABLE `{project_id}.vendas.asaas_customers_gold`
AS
SELECT *
FROM `{project_id}.asaas.customers`;

# SUBSCRIPTIONS
CREATE OR REPLACE TABLE `{project_id}.vendas.asaas_subscriptions_gold`
AS
SELECT *
FROM `{project_id}.asaas.subscriptions`;

# SUBSCRIPTIONS_ID_PAYMENTS
CREATE OR REPLACE TABLE `{project_id}.vendas.asaas_subscriptions_id_payments_gold`
AS
SELECT *
FROM `{project_id}.asaas.subscriptions_id_payments`;