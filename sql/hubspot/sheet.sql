# HUBSPOT_CONTACTS
CREATE OR REPLACE EXTERNAL TABLE takeat-app-nalk.hubspot.hubspot_contacts
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://hubspot-takeat/hubspot_contacts/hubspot_contacts.csv']);

# HUBSPOT_DEALS
CREATE OR REPLACE EXTERNAL TABLE takeat-app-nalk.hubspot.hubspot_deals
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://hubspot-takeat/hubspot_deals/hubspot_deals.csv']);

# HUBSPOT_DEALS_PROPERTIES
CREATE OR REPLACE EXTERNAL TABLE takeat-app-nalk.hubspot.hubspot_deals_properties
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://hubspot-takeat/hubspot_deals_properties/hubspot_deals_properties.csv']);

# HUBSPOT_OWNERS
CREATE OR REPLACE EXTERNAL TABLE takeat-app-nalk.hubspot.hubspot_owners
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://hubspot-takeat/hubspot_owners/hubspot_owners.csv']);

# HUBSPOT_PIPELINES
CREATE OR REPLACE EXTERNAL TABLE takeat-app-nalk.hubspot.hubspot_pipelines
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://hubspot-takeat/hubspot_pipelines/hubspot_pipelines.csv']);

# HUBSPOT_PROPERTIES
CREATE OR REPLACE EXTERNAL TABLE takeat-app-nalk.hubspot.hubspot_properties
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://hubspot-takeat/hubspot_properties/hubspot_properties.csv']);

-- GOLD
# HUBSPOT_CONTACTS
CREATE OR REPLACE TABLE `takeat-app-nalk.vendas.hubspot_hubspot_contacts_gold`
AS
SELECT *
FROM `takeat-app-nalk.hubspot.hubspot_contacts`;

# HUBSPOT_DEALS
CREATE OR REPLACE TABLE `takeat-app-nalk.vendas.hubspot_hubspot_deals_gold`
AS
SELECT *
FROM `takeat-app-nalk.hubspot.hubspot_deals`;

# HUBSPOT_DEALS_PROPERTIES
CREATE OR REPLACE TABLE `takeat-app-nalk.vendas.hubspot_hubspot_deals_properties_gold`
AS
SELECT *
FROM `takeat-app-nalk.hubspot.hubspot_deals_properties`;

# HUBSPOT_OWNERS
CREATE OR REPLACE TABLE `takeat-app-nalk.vendas.hubspot_hubspot_owners_gold`
AS
SELECT *
FROM `takeat-app-nalk.hubspot.hubspot_owners`;

# HUBSPOT_PIPELINES
CREATE OR REPLACE TABLE `takeat-app-nalk.vendas.hubspot_hubspot_pipelines_gold`
AS
SELECT *
FROM `takeat-app-nalk.hubspot.hubspot_pipelines`;

# HUBSPOT_PROPERTIES
CREATE OR REPLACE TABLE `takeat-app-nalk.vendas.hubspot_hubspot_properties_gold`
AS
SELECT *
FROM `takeat-app-nalk.hubspot.hubspot_properties`;