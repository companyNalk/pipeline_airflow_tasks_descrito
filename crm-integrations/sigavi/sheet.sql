# FAC_LIST
CREATE OR REPLACE EXTERNAL TABLE {project_id}.sigavi.fac_list
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/fac_list/fac_list.csv']);

-- GOLD
# FAC_LIST
CREATE OR REPLACE TABLE `{project_id}.vendas.sigavi_fac_list_gold`
AS
SELECT *
FROM `{project_id}.sigavi.fac_list`;