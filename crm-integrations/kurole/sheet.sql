# CLIENTES
CREATE OR REPLACE EXTERNAL TABLE {project_id}.kurole.clientes
OPTIONS (
  format = 'CSV',
  field_delimiter=',',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/clientes/clientes.csv']);

# CLIENTES_IMOVEIS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.kurole.clientes_imoveis
OPTIONS (
  format = 'CSV',
  field_delimiter=',',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/clientes_imoveis/clientes_imoveis.csv']);

# ORDEM_ATENDIMENTO
CREATE OR REPLACE EXTERNAL TABLE {project_id}.kurole.ordem_atendimento
OPTIONS (
  format = 'CSV',
  field_delimiter=',',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/ordem_atendimento/ordem_atendimento.csv']);

# CONTRATO_VENDA_DOCUMENTOS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.kurole.contrato_venda_documentos
OPTIONS (
  format = 'CSV',
  field_delimiter=',',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/contrato_venda_documentos/contrato_venda_documentos.csv']);

# CONTRATO_DISTRATO
CREATE OR REPLACE EXTERNAL TABLE {project_id}.kurole.contrato_distrato
OPTIONS (
  format = 'CSV',
  field_delimiter=',',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/contrato_distrato/contrato_distrato.csv']);

# IMOVEIS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.kurole.imoveis
OPTIONS (
  format = 'CSV',
  field_delimiter=',',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/imoveis/imoveis.csv']);

# OCORRENCIAS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.kurole.ocorrencias
OPTIONS (
  format = 'CSV',
  field_delimiter=',',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/ocorrencias/ocorrencias.csv']);

# RD_STATION_LEADS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.kurole.rd_station_leads
OPTIONS (
  format = 'CSV',
  field_delimiter=',',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/rd_station_leads/rd_station_leads.csv']);

# AUTOMACAO_LEADS_INTEGRACAO
CREATE OR REPLACE EXTERNAL TABLE {project_id}.kurole.automacao_leads_integracao
OPTIONS (
  format = 'CSV',
  field_delimiter=',',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/automacao_leads_integracao/automacao_leads_integracao.csv']);

-- GOLD
# CLIENTES
CREATE OR REPLACE TABLE `{project_id}.vendas.kurole_clientes_gold`
AS
SELECT *
FROM `{project_id}.kurole.clientes`;

# CLIENTES_IMOVEIS
CREATE OR REPLACE TABLE `{project_id}.vendas.kurole_clientes_imoveis_gold`
AS
SELECT *
FROM `{project_id}.kurole.clientes_imoveis`;

# ORDEM_ATENDIMENTO
CREATE OR REPLACE TABLE `{project_id}.vendas.kurole_ordem_atendimento_gold`
AS
SELECT *
FROM `{project_id}.kurole.ordem_atendimento`;

# CONTRATO_VENDA_DOCUMENTOS
CREATE OR REPLACE TABLE `{project_id}.vendas.kurole_contrato_venda_documentos_gold`
AS
SELECT *
FROM `{project_id}.kurole.contrato_venda_documentos`;

# CONTRATO_DISTRATO
CREATE OR REPLACE TABLE `{project_id}.vendas.kurole_contrato_distrato_gold`
AS
SELECT *
FROM `{project_id}.kurole.contrato_distrato`;

# IMOVEIS
CREATE OR REPLACE TABLE `{project_id}.vendas.kurole_imoveis_gold`
AS
SELECT *
FROM `{project_id}.kurole.imoveis`;

# OCORRENCIAS
CREATE OR REPLACE TABLE `{project_id}.vendas.kurole_ocorrencias_gold`
AS
SELECT *
FROM `{project_id}.kurole.ocorrencias`;

# RD_STATION_LEADS
CREATE OR REPLACE TABLE `{project_id}.vendas.kurole_rd_station_leads_gold`
AS
SELECT *
FROM `{project_id}.kurole.rd_station_leads`;

# AUTOMACAO_LEADS_INTEGRACAO
CREATE OR REPLACE TABLE `{project_id}.vendas.kurole_automacao_leads_integracao_gold`
AS
SELECT *
FROM `{project_id}.kurole.automacao_leads_integracao`;