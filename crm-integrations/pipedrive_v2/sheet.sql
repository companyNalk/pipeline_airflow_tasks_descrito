# ACTIVITIES
CREATE OR REPLACE EXTERNAL TABLE {project_id}.pipedrive.activities
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/activities/activities.csv']);

# ACTIVITY_FIELDS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.pipedrive.activity_fields
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/activity_fields/activity_fields.csv']);

# DEALS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.pipedrive.deals
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/deals/deals.csv']);

# DEAL_FIELDS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.pipedrive.deal_fields
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/deal_fields/deal_fields.csv']);

# LEADS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.pipedrive.leads
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/leads/leads.csv']);

# LEAD_FIELDS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.pipedrive.lead_fields
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/lead_fields/lead_fields.csv']);

# ORGANIZATIONS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.pipedrive.organizations
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/organizations/organizations.csv']);

# ORGANIZATION_FIELDS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.pipedrive.organization_fields
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/organization_fields/organization_fields.csv']);

# PERSONS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.pipedrive.persons
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/persons/persons.csv']);

# PERSON_FIELD
CREATE OR REPLACE EXTERNAL TABLE {project_id}.pipedrive.person_field
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/person_field/person_field.csv']);

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

# PRODUCT_FIELDS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.pipedrive.product_fields
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/product_fields/product_fields.csv']);

# STAGES
CREATE OR REPLACE EXTERNAL TABLE {project_id}.pipedrive.stages
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/stages/stages.csv']);

# USERS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.pipedrive.users
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/users/users.csv']);

-- GOLD
# ACTIVITIES
CREATE OR REPLACE TABLE `{project_id}.vendas.pipedrive_activities_gold`
AS
SELECT *
FROM `{project_id}.pipedrive.activities`;

# ACTIVITY_FIELDS
CREATE OR REPLACE TABLE `{project_id}.vendas.pipedrive_activity_fields_gold`
AS
SELECT *
FROM `{project_id}.pipedrive.activity_fields`;

# DEALS
CREATE OR REPLACE TABLE `{project_id}.vendas.pipedrive_deals_gold`
AS
SELECT *
FROM `{project_id}.pipedrive.deals`;

# DEAL_FIELDS
CREATE OR REPLACE TABLE `{project_id}.vendas.pipedrive_deal_fields_gold`
AS
SELECT *
FROM `{project_id}.pipedrive.deal_fields`;

# LEADS
CREATE OR REPLACE TABLE `{project_id}.vendas.pipedrive_leads_gold`
AS
SELECT *
FROM `{project_id}.pipedrive.leads`;

# LEAD_FIELDS
CREATE OR REPLACE TABLE `{project_id}.vendas.pipedrive_lead_fields_gold`
AS
SELECT *
FROM `{project_id}.pipedrive.lead_fields`;

# ORGANIZATIONS
CREATE OR REPLACE TABLE `{project_id}.vendas.pipedrive_organizations_gold`
AS
SELECT *
FROM `{project_id}.pipedrive.organizations`;

# ORGANIZATION_FIELDS
CREATE OR REPLACE TABLE `{project_id}.vendas.pipedrive_organization_fields_gold`
AS
SELECT *
FROM `{project_id}.pipedrive.organization_fields`;

# PERSONS
CREATE OR REPLACE TABLE `{project_id}.vendas.pipedrive_persons_gold`
AS
SELECT *
FROM `{project_id}.pipedrive.persons`;

# PERSON_FIELD
CREATE OR REPLACE TABLE `{project_id}.vendas.pipedrive_person_field_gold`
AS
SELECT *
FROM `{project_id}.pipedrive.person_field`;

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

# PRODUCT_FIELDS
CREATE OR REPLACE TABLE `{project_id}.vendas.pipedrive_product_fields_gold`
AS
SELECT *
FROM `{project_id}.pipedrive.product_fields`;

# STAGES
CREATE OR REPLACE TABLE `{project_id}.vendas.pipedrive_stages_gold`
AS
SELECT *
FROM `{project_id}.pipedrive.stages`;

# USERS
CREATE OR REPLACE TABLE `{project_id}.vendas.pipedrive_users_gold`
AS
SELECT *
FROM `{project_id}.pipedrive.users`;