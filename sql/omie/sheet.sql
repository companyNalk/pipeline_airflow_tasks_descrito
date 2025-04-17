# LISTAR_CATEGORIAS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.omie.listar_categorias
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/listar_categorias/listar_categorias.csv']);

# LISTAR_PRODUTOS_PEDIDOS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.omie.listar_produtos_pedidos
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/listar_produtos_pedidos/listar_produtos_pedidos.csv']);

# VENDEDORES
CREATE OR REPLACE EXTERNAL TABLE {project_id}.omie.vendedores
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/vendedores/vendedores.csv']);

# LISTAR_PRODUTOS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.omie.listar_produtos
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/listar_produtos/listar_produtos.csv']);

# LISTAR_MOTIVO_DEVOLUCAO
CREATE OR REPLACE EXTERNAL TABLE {project_id}.omie.listar_motivo_devolucao
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/listar_motivo_devolucao/listar_motivo_devolucao.csv']);

# LISTAR_PEDIDOS_ETAPAS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.omie.listar_pedidos_etapas
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/listar_pedidos_etapas/listar_pedidos_etapas.csv']);

# ETAPAFAT
CREATE OR REPLACE EXTERNAL TABLE {project_id}.omie.etapafat
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/etapafat/etapafat.csv']);

# PEDIDOS_VENDAS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.omie.pedidos_vendas
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/pedidos_vendas/pedidos_vendas.csv']);

-- GOLD
# LISTAR_CATEGORIAS
CREATE OR REPLACE TABLE `{project_id}.vendas.omie_listar_categorias_gold`
AS
SELECT *
FROM `{project_id}.omie.listar_categorias`;

# LISTAR_PRODUTOS_PEDIDOS
CREATE OR REPLACE TABLE `{project_id}.vendas.omie_listar_produtos_pedidos_gold`
AS
SELECT *
FROM `{project_id}.omie.listar_produtos_pedidos`;

# VENDEDORES
CREATE OR REPLACE TABLE `{project_id}.vendas.omie_vendedores_gold`
AS
SELECT *
FROM `{project_id}.omie.vendedores`;

# LISTAR_PRODUTOS
CREATE OR REPLACE TABLE `{project_id}.vendas.omie_listar_produtos_gold`
AS
SELECT *
FROM `{project_id}.omie.listar_produtos`;

# LISTAR_MOTIVO_DEVOLUCAO
CREATE OR REPLACE TABLE `{project_id}.vendas.omie_listar_motivo_devolucao_gold`
AS
SELECT *
FROM `{project_id}.omie.listar_motivo_devolucao`;

# LISTAR_PEDIDOS_ETAPAS
CREATE OR REPLACE TABLE `{project_id}.vendas.omie_listar_pedidos_etapas_gold`
AS
SELECT *
FROM `{project_id}.omie.listar_pedidos_etapas`;

# ETAPAFAT
CREATE OR REPLACE TABLE `{project_id}.vendas.omie_etapafat_gold`
AS
SELECT *
FROM `{project_id}.omie.etapafat`;

# PEDIDOS_VENDAS
CREATE OR REPLACE TABLE `{project_id}.vendas.omie_pedidos_vendas_gold`
AS
SELECT *
FROM `{project_id}.omie.pedidos_vendas`;