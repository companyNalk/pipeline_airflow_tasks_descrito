# SERVICES
CREATE OR REPLACE EXTERNAL TABLE {project_id}.imoview.services
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/services/services.csv']);

# EXTRA_FIELDS_AVAILABLE
CREATE OR REPLACE EXTERNAL TABLE {project_id}.imoview.extra_fields_available
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/extra_fields_available/extra_fields_available.csv']);

# BUSINESS_BUYER
CREATE OR REPLACE EXTERNAL TABLE {project_id}.imoview.business_buyer
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/business_buyer/business_buyer.csv']);

# TENANT_CONTRACTS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.imoview.tenant_contracts
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/tenant_contracts/tenant_contracts.csv']);

# TENANT_PROPERTIES
CREATE OR REPLACE EXTERNAL TABLE {project_id}.imoview.tenant_properties
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/tenant_properties/tenant_properties.csv']);

# LIST_USERS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.imoview.list_users
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/list_users/list_users.csv']);

# WALLETS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.imoview.wallets
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/wallets/wallets.csv']);

# WORKFLOWS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.imoview.workflows
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/workflows/workflows.csv']);

-- GOLD
# SERVICES
CREATE OR REPLACE TABLE `{project_id}.vendas.imoview_services_gold`
AS
SELECT *
FROM `{project_id}.imoview.services`;

# EXTRA_FIELDS_AVAILABLE
CREATE OR REPLACE TABLE `{project_id}.vendas.imoview_extra_fields_available_gold`
AS
SELECT *
FROM `{project_id}.imoview.extra_fields_available`;

# BUSINESS_BUYER
CREATE OR REPLACE TABLE `{project_id}.vendas.imoview_business_buyer_gold`
AS
SELECT *
FROM `{project_id}.imoview.business_buyer`;

# TENANT_CONTRACTS
CREATE OR REPLACE TABLE `{project_id}.vendas.imoview_tenant_contracts_gold`
AS
SELECT *
FROM `{project_id}.imoview.tenant_contracts`;

# TENANT_PROPERTIES
CREATE OR REPLACE TABLE `{project_id}.vendas.imoview_tenant_properties_gold`
AS
SELECT *
FROM `{project_id}.imoview.tenant_properties`;

# LIST_USERS
CREATE OR REPLACE TABLE `{project_id}.vendas.imoview_list_users_gold`
AS
SELECT *
FROM `{project_id}.imoview.list_users`;

# WALLETS
CREATE OR REPLACE TABLE `{project_id}.vendas.imoview_wallets_gold`
AS
SELECT *
FROM `{project_id}.imoview.wallets`;

# WORKFLOWS
CREATE OR REPLACE TABLE `{project_id}.vendas.imoview_workflows_gold`
AS
SELECT *
FROM `{project_id}.imoview.workflows`;