# ACTIVITIES
CREATE OR REPLACE EXTERNAL TABLE {project_id}.pipedrive_v2.activities
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/activities/activities.csv']);

# ACTIVITY_FIELDS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.pipedrive_v2.activity_fields
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/activity_fields/activity_fields.csv']);

# DEALS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.pipedrive_v2.deals
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/deals/deals.csv']);

# DEAL_FIELDS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.pipedrive_v2.deal_fields
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/deal_fields/deal_fields.csv']);

# LEADS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.pipedrive_v2.leads
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/leads/leads.csv']);

# LEAD_FIELDS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.pipedrive_v2.lead_fields
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/lead_fields/lead_fields.csv']);

# ORGANIZATIONS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.pipedrive_v2.organizations
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/organizations/organizations.csv']);

# ORGANIZATION_FIELDS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.pipedrive_v2.organization_fields
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/organization_fields/organization_fields.csv']);

# PERSONS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.pipedrive_v2.persons
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/persons/persons.csv']);

# PERSON_FIELD
CREATE OR REPLACE EXTERNAL TABLE {project_id}.pipedrive_v2.person_field
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/person_field/person_field.csv']);

# PIPELINES
CREATE OR REPLACE EXTERNAL TABLE {project_id}.pipedrive_v2.pipelines
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/pipelines/pipelines.csv']);

# PRODUCTS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.pipedrive_v2.products
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/products/products.csv']);

# PRODUCT_FIELDS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.pipedrive_v2.product_fields
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/product_fields/product_fields.csv']);

# STAGES
CREATE OR REPLACE EXTERNAL TABLE {project_id}.pipedrive_v2.stages
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/stages/stages.csv']);

# USERS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.pipedrive_v2.users
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/users/users.csv']);

-- GOLD
# ACTIVITIES
CREATE OR REPLACE TABLE `{project_id}.vendas.pipedrive_v2_activities_gold`
AS
SELECT *
FROM `{project_id}.pipedrive_v2.activities`;

# ACTIVITY_FIELDS
CREATE OR REPLACE TABLE `{project_id}.vendas.pipedrive_v2_activity_fields_gold`
AS
SELECT *
FROM `{project_id}.pipedrive_v2.activity_fields`;

# DEALS
CREATE OR REPLACE TABLE `{project_id}.vendas.pipedrive_v2_deals_gold`
AS
SELECT *
FROM `{project_id}.pipedrive_v2.deals`;

# DEAL_FIELDS
CREATE OR REPLACE TABLE `{project_id}.vendas.pipedrive_v2_deal_fields_gold`
AS
SELECT *
FROM `{project_id}.pipedrive_v2.deal_fields`;

# LEADS
CREATE OR REPLACE TABLE `{project_id}.vendas.pipedrive_v2_leads_gold`
AS
SELECT *
FROM `{project_id}.pipedrive_v2.leads`;

# LEAD_FIELDS
CREATE OR REPLACE TABLE `{project_id}.vendas.pipedrive_v2_lead_fields_gold`
AS
SELECT *
FROM `{project_id}.pipedrive_v2.lead_fields`;

# ORGANIZATIONS
CREATE OR REPLACE TABLE `{project_id}.vendas.pipedrive_v2_organizations_gold`
AS
SELECT *
FROM `{project_id}.pipedrive_v2.organizations`;

# ORGANIZATION_FIELDS
CREATE OR REPLACE TABLE `{project_id}.vendas.pipedrive_v2_organization_fields_gold`
AS
SELECT *
FROM `{project_id}.pipedrive_v2.organization_fields`;

# PERSONS
CREATE OR REPLACE TABLE `{project_id}.vendas.pipedrive_v2_persons_gold`
AS
SELECT *
FROM `{project_id}.pipedrive_v2.persons`;

# PERSON_FIELD
CREATE OR REPLACE TABLE `{project_id}.vendas.pipedrive_v2_person_field_gold`
AS
SELECT *
FROM `{project_id}.pipedrive_v2.person_field`;

# PIPELINES
CREATE OR REPLACE TABLE `{project_id}.vendas.pipedrive_v2_pipelines_gold`
AS
SELECT *
FROM `{project_id}.pipedrive_v2.pipelines`;

# PRODUCTS
CREATE OR REPLACE TABLE `{project_id}.vendas.pipedrive_v2_products_gold`
AS
SELECT *
FROM `{project_id}.pipedrive_v2.products`;

# PRODUCT_FIELDS
CREATE OR REPLACE TABLE `{project_id}.vendas.pipedrive_v2_product_fields_gold`
AS
SELECT *
FROM `{project_id}.pipedrive_v2.product_fields`;

# STAGES
CREATE OR REPLACE TABLE `{project_id}.vendas.pipedrive_v2_stages_gold`
AS
SELECT *
FROM `{project_id}.pipedrive_v2.stages`;

# USERS
CREATE OR REPLACE TABLE `{project_id}.vendas.pipedrive_v2_users_gold`
AS
SELECT *
FROM `{project_id}.pipedrive_v2.users`;