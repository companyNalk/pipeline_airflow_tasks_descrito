# QUALIFICACOES
CREATE OR REPLACE EXTERNAL TABLE {project_id}.nectar.qualificacoes
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/qualificacoes/qualificacoes.csv']);

# OPORTUNIDADES
CREATE OR REPLACE EXTERNAL TABLE {project_id}.nectar.oportunidades
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/oportunidades/oportunidades.csv']);

-- GOLD
# QUALIFICACOES
CREATE OR REPLACE TABLE `{project_id}.vendas.nectar_qualificacoes_gold`
AS
SELECT *
FROM `{project_id}.nectar.qualificacoes`;

# OPORTUNIDADES
CREATE OR REPLACE TABLE `{project_id}.vendas.nectar_oportunidades_gold`
AS
SELECT *
FROM `{project_id}.nectar.oportunidades`;