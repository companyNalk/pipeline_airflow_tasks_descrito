# DEALS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.active_campaign_crm.deals
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/deals/deals.csv']);

# DEAL_GROUPS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.active_campaign_crm.deal_groups
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/deal_groups/deal_groups.csv']);

# DEAL_STAGES
CREATE OR REPLACE EXTERNAL TABLE {project_id}.active_campaign_crm.deal_stages
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/deal_stages/deal_stages.csv']);

# DEAL_CUSTOM_FIELD_META
CREATE OR REPLACE EXTERNAL TABLE {project_id}.active_campaign_crm.deal_custom_field_meta
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/deal_custom_field_meta/deal_custom_field_meta.csv']);

# DEAL_CUSTOM_FIELD_DATA
CREATE OR REPLACE EXTERNAL TABLE {project_id}.active_campaign_crm.deal_custom_field_data
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/deal_custom_field_data/deal_custom_field_data.csv']);

-- GOLD
# DEALS
CREATE OR REPLACE TABLE `{project_id}.vendas.active_campaign_crm_deals_gold`
AS
SELECT *
FROM `{project_id}.active_campaign_crm.deals`;

# DEAL_GROUPS
CREATE OR REPLACE TABLE `{project_id}.vendas.active_campaign_crm_deal_groups_gold`
AS
SELECT *
FROM `{project_id}.active_campaign_crm.deal_groups`;

# DEAL_STAGES
CREATE OR REPLACE TABLE `{project_id}.vendas.active_campaign_crm_deal_stages_gold`
AS
SELECT *
FROM `{project_id}.active_campaign_crm.deal_stages`;

# DEAL_CUSTOM_FIELD_META
CREATE OR REPLACE TABLE `{project_id}.vendas.active_campaign_crm_deal_custom_field_meta_gold`
AS
SELECT *
FROM `{project_id}.active_campaign_crm.deal_custom_field_meta`;

# DEAL_CUSTOM_FIELD_DATA
CREATE OR REPLACE TABLE `{project_id}.vendas.active_campaign_crm_deal_custom_field_data_gold`
AS
SELECT *
FROM `{project_id}.active_campaign_crm.deal_custom_field_data`;