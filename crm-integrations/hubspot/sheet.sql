# HUBSPOT_CONTACTS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.hubspot.hubspot_contacts
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/hubspot_contacts/hubspot_contacts.csv']);

# HUBSPOT_DEALS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.hubspot.hubspot_deals
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/hubspot_deals/hubspot_deals.csv']);

# HUBSPOT_OWNERS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.hubspot.hubspot_owners
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/hubspot_owners/hubspot_owners.csv']);

# HUBSPOT_PIPELINES
CREATE OR REPLACE EXTERNAL TABLE {project_id}.hubspot.hubspot_pipelines
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/hubspot_pipelines/hubspot_pipelines.csv']);

# HUBSPOT_PROPERTIES
CREATE OR REPLACE EXTERNAL TABLE {project_id}.hubspot.hubspot_properties
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/hubspot_properties/hubspot_properties.csv']);

-- GOLD
# HUBSPOT_CONTACTS
CREATE OR REPLACE TABLE `{project_id}.vendas.hubspot_hubspot_contacts_gold`
AS
SELECT *
FROM `{project_id}.hubspot.hubspot_contacts`;

# HUBSPOT_DEALS
CREATE OR REPLACE TABLE `{project_id}.vendas.hubspot_hubspot_deals_gold`
AS
SELECT *
FROM `{project_id}.hubspot.hubspot_deals`;

# HUBSPOT_OWNERS
CREATE OR REPLACE TABLE `{project_id}.vendas.hubspot_hubspot_owners_gold`
AS
SELECT *
FROM `{project_id}.hubspot.hubspot_owners`;

# HUBSPOT_PIPELINES
CREATE OR REPLACE TABLE `{project_id}.vendas.hubspot_hubspot_pipelines_gold`
AS
SELECT *
FROM `{project_id}.hubspot.hubspot_pipelines`;

# HUBSPOT_PROPERTIES
CREATE OR REPLACE TABLE `{project_id}.vendas.hubspot_hubspot_properties_gold`
AS
SELECT *
FROM `{project_id}.hubspot.hubspot_properties`;