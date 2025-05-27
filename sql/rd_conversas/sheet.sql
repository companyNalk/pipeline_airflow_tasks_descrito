# CUSTOMERS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.rd_conversas.customers
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/customers/customers.csv']);

# FLOWS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.rd_conversas.flows
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/flows/flows.csv']);

# WHATSAPP_INTEGRATIONS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.rd_conversas.whatsapp_integrations
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/whatsapp_integrations/whatsapp_integrations.csv']);

# WHATSAPP_INTEGRATIONS_OFFICIAL
CREATE OR REPLACE EXTERNAL TABLE {project_id}.rd_conversas.whatsapp_integrations_official
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/whatsapp_integrations_official/whatsapp_integrations_official.csv']);

# REPORTS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.rd_conversas.reports
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/reports/reports.csv']);

# TEMPLATE
CREATE OR REPLACE EXTERNAL TABLE {project_id}.rd_conversas.template
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/template/template.csv']);

# WALLETS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.rd_conversas.wallets
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/wallets/wallets.csv']);

# WORKFLOWS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.rd_conversas.workflows
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/workflows/workflows.csv']);

-- GOLD
# CUSTOMERS
CREATE OR REPLACE TABLE `{project_id}.vendas.rd_conversas_customers_gold`
AS
SELECT *
FROM `{project_id}.rd_conversas.customers`;

# FLOWS
CREATE OR REPLACE TABLE `{project_id}.vendas.rd_conversas_flows_gold`
AS
SELECT *
FROM `{project_id}.rd_conversas.flows`;

# WHATSAPP_INTEGRATIONS
CREATE OR REPLACE TABLE `{project_id}.vendas.rd_conversas_whatsapp_integrations_gold`
AS
SELECT *
FROM `{project_id}.rd_conversas.whatsapp_integrations`;

# WHATSAPP_INTEGRATIONS_OFFICIAL
CREATE OR REPLACE TABLE `{project_id}.vendas.rd_conversas_whatsapp_integrations_official_gold`
AS
SELECT *
FROM `{project_id}.rd_conversas.whatsapp_integrations_official`;

# REPORTS
CREATE OR REPLACE TABLE `{project_id}.vendas.rd_conversas_reports_gold`
AS
SELECT *
FROM `{project_id}.rd_conversas.reports`;

# TEMPLATE
CREATE OR REPLACE TABLE `{project_id}.vendas.rd_conversas_template_gold`
AS
SELECT *
FROM `{project_id}.rd_conversas.template`;

# WALLETS
CREATE OR REPLACE TABLE `{project_id}.vendas.rd_conversas_wallets_gold`
AS
SELECT *
FROM `{project_id}.rd_conversas.wallets`;

# WORKFLOWS
CREATE OR REPLACE TABLE `{project_id}.vendas.rd_conversas_workflows_gold`
AS
SELECT *
FROM `{project_id}.rd_conversas.workflows`;