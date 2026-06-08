# PESSOAS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.contaazul.pessoas
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/pessoas/pessoas.csv']);

# PRODUTOS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.contaazul.produtos
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/produtos/produtos.csv']);

# VENDAS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.contaazul.vendas
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/vendas/vendas.csv']);

# FINANCEIRO_EVENTOS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.contaazul.financeiro_eventos
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/financeiro_eventos/financeiro_eventos.csv']);

-- GOLD
# PESSOAS
CREATE OR REPLACE TABLE `{project_id}.vendas.contaazul_pessoas_gold`
AS
SELECT *
FROM `{project_id}.contaazul.pessoas`;

# PRODUTOS
CREATE OR REPLACE TABLE `{project_id}.vendas.contaazul_produtos_gold`
AS
SELECT *
FROM `{project_id}.contaazul.produtos`;

# VENDAS
CREATE OR REPLACE TABLE `{project_id}.vendas.contaazul_vendas_gold`
AS
SELECT *
FROM `{project_id}.contaazul.vendas`;

# FINANCEIRO_EVENTOS
CREATE OR REPLACE TABLE `{project_id}.vendas.contaazul_financeiro_eventos_gold`
AS
SELECT *
FROM `{project_id}.contaazul.financeiro_eventos`;
