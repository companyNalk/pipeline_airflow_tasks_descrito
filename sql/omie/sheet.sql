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

# CONTAS_CORRENTES
CREATE OR REPLACE EXTERNAL TABLE {project_id}.omie.contas_correntes
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/contas_correntes/contas_correntes.csv']);

# CONTAS_RECEBER
CREATE OR REPLACE EXTERNAL TABLE {project_id}.omie.contas_receber
OPTIONS (
  format = 'CSV',
  field_delimiter='|',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/contas_receber/contas_receber.csv']);

# CONTAS_PAGAR
CREATE OR REPLACE EXTERNAL TABLE {project_id}.omie.contas_pagar
OPTIONS (
  format = 'CSV',
  field_delimiter='|',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/contas_pagar/contas_pagar.csv']);

# LANCAMENTOS_CONTA_CORRENTE
CREATE OR REPLACE EXTERNAL TABLE {project_id}.omie.lancamentos_conta_corrente
OPTIONS (
  format = 'CSV',
  field_delimiter='|',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/lancamentos_conta_corrente/lancamentos_conta_corrente.csv']);

# MOVIMENTOS_FINANCEIROS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.omie.movimentos_financeiros
OPTIONS (
  format = 'CSV',
  field_delimiter='|',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/movimentos_financeiros/movimentos_financeiros.csv']);

# CLIENTES
CREATE OR REPLACE EXTERNAL TABLE {project_id}.omie.clientes
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/clientes/clientes.csv']);

# PROJETOS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.omie.projetos
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/projetos/projetos.csv']);

# CRM_CONTAS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.omie.crm_contas
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/crm_contas/crm_contas.csv']);

# CRM_CONTATOS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.omie.crm_contatos
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/crm_contatos/crm_contatos.csv']);

# CRM_OPORTUNIDADES
CREATE OR REPLACE EXTERNAL TABLE {project_id}.omie.crm_oportunidades
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/crm_oportunidades/crm_oportunidades.csv']);

# CRM_TAREFAS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.omie.crm_tarefas
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/crm_tarefas/crm_tarefas.csv']);

# SERVICOS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.omie.servicos
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/servicos/servicos.csv']);

# ORDENS_SERVICO
CREATE OR REPLACE EXTERNAL TABLE {project_id}.omie.ordens_servico
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/ordens_servico/ordens_servico.csv']);

# CONTRATOS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.omie.contratos
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/contratos/contratos.csv']);

# NFSE
CREATE OR REPLACE EXTERNAL TABLE {project_id}.omie.nfse
OPTIONS (
  format = 'CSV',
  field_delimiter='|',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/nfse/nfse.csv']);

# PEDIDOS_COMPRA
CREATE OR REPLACE EXTERNAL TABLE {project_id}.omie.pedidos_compra
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/pedidos_compra/pedidos_compra.csv']);

# NOTAS_ENTRADA
CREATE OR REPLACE EXTERNAL TABLE {project_id}.omie.notas_entrada
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/notas_entrada/notas_entrada.csv']);

# NFE
CREATE OR REPLACE EXTERNAL TABLE {project_id}.omie.nfe
OPTIONS (
  format = 'CSV',
  field_delimiter='|',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/nfe/nfe.csv']);

# CTE
CREATE OR REPLACE EXTERNAL TABLE {project_id}.omie.cte
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/cte/cte.csv']);

# EXTRATO
CREATE OR REPLACE EXTERNAL TABLE {project_id}.omie.extrato
OPTIONS (
  format = 'CSV',
  field_delimiter='|',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/extrato/extrato.csv']);

# MOVIMENTOS_ESTOQUE
CREATE OR REPLACE EXTERNAL TABLE {project_id}.omie.movimentos_estoque
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/movimentos_estoque/movimentos_estoque.csv']);

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

# CONTAS_CORRENTES
CREATE OR REPLACE TABLE `{project_id}.vendas.omie_contas_correntes_gold`
AS
SELECT *
FROM `{project_id}.omie.contas_correntes`;

# CONTAS_RECEBER
CREATE OR REPLACE TABLE `{project_id}.vendas.omie_contas_receber_gold`
AS
SELECT *
FROM `{project_id}.omie.contas_receber`;

# CONTAS_PAGAR
CREATE OR REPLACE TABLE `{project_id}.vendas.omie_contas_pagar_gold`
AS
SELECT *
FROM `{project_id}.omie.contas_pagar`;

# LANCAMENTOS_CONTA_CORRENTE
CREATE OR REPLACE TABLE `{project_id}.vendas.omie_lancamentos_conta_corrente_gold`
AS
SELECT *
FROM `{project_id}.omie.lancamentos_conta_corrente`;

# MOVIMENTOS_FINANCEIROS
CREATE OR REPLACE TABLE `{project_id}.vendas.omie_movimentos_financeiros_gold`
AS
SELECT *
FROM `{project_id}.omie.movimentos_financeiros`;

# CLIENTES
CREATE OR REPLACE TABLE `{project_id}.vendas.omie_clientes_gold`
AS SELECT * FROM `{project_id}.omie.clientes`;

# PROJETOS
CREATE OR REPLACE TABLE `{project_id}.vendas.omie_projetos_gold`
AS SELECT * FROM `{project_id}.omie.projetos`;

# CRM_CONTAS
CREATE OR REPLACE TABLE `{project_id}.vendas.omie_crm_contas_gold`
AS SELECT * FROM `{project_id}.omie.crm_contas`;

# CRM_CONTATOS
CREATE OR REPLACE TABLE `{project_id}.vendas.omie_crm_contatos_gold`
AS SELECT * FROM `{project_id}.omie.crm_contatos`;

# CRM_OPORTUNIDADES
CREATE OR REPLACE TABLE `{project_id}.vendas.omie_crm_oportunidades_gold`
AS SELECT * FROM `{project_id}.omie.crm_oportunidades`;

# CRM_TAREFAS
CREATE OR REPLACE TABLE `{project_id}.vendas.omie_crm_tarefas_gold`
AS SELECT * FROM `{project_id}.omie.crm_tarefas`;

# SERVICOS
CREATE OR REPLACE TABLE `{project_id}.vendas.omie_servicos_gold`
AS SELECT * FROM `{project_id}.omie.servicos`;

# ORDENS_SERVICO
CREATE OR REPLACE TABLE `{project_id}.vendas.omie_ordens_servico_gold`
AS SELECT * FROM `{project_id}.omie.ordens_servico`;

# CONTRATOS
CREATE OR REPLACE TABLE `{project_id}.vendas.omie_contratos_gold`
AS SELECT * FROM `{project_id}.omie.contratos`;

# NFSE
CREATE OR REPLACE TABLE `{project_id}.vendas.omie_nfse_gold`
AS SELECT * FROM `{project_id}.omie.nfse`;

# PEDIDOS_COMPRA
CREATE OR REPLACE TABLE `{project_id}.vendas.omie_pedidos_compra_gold`
AS SELECT * FROM `{project_id}.omie.pedidos_compra`;

# NOTAS_ENTRADA
CREATE OR REPLACE TABLE `{project_id}.vendas.omie_notas_entrada_gold`
AS SELECT * FROM `{project_id}.omie.notas_entrada`;

# NFE
CREATE OR REPLACE TABLE `{project_id}.vendas.omie_nfe_gold`
AS SELECT * FROM `{project_id}.omie.nfe`;

# CTE
CREATE OR REPLACE TABLE `{project_id}.vendas.omie_cte_gold`
AS SELECT * FROM `{project_id}.omie.cte`;

# EXTRATO
CREATE OR REPLACE TABLE `{project_id}.vendas.omie_extrato_gold`
AS SELECT * FROM `{project_id}.omie.extrato`;

# MOVIMENTOS_ESTOQUE
CREATE OR REPLACE TABLE `{project_id}.vendas.omie_movimentos_estoque_gold`
AS SELECT * FROM `{project_id}.omie.movimentos_estoque`;