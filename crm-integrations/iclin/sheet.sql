# UNIDADES
CREATE OR REPLACE EXTERNAL TABLE {project_id}.iclin.unidades
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/unidades/unidades.csv']);

# AGENDAS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.iclin.agendas
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/agendas/agendas.csv']);

# CONVENIOS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.iclin.convenios
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/convenios/convenios.csv']);

# ATENDIMENTOS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.iclin.atendimentos
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/atendimentos/atendimentos.csv']);

# ATENDIMENTO_SERVICOS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.iclin.atendimento_servicos
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/atendimento_servicos/atendimento_servicos.csv']);

# CLIENTES
CREATE OR REPLACE EXTERNAL TABLE {project_id}.iclin.clientes
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/clientes/clientes.csv']);

-- GOLD
# UNIDADES
CREATE OR REPLACE TABLE `{project_id}.vendas.iclin_unidades_gold`
AS SELECT * FROM `{project_id}.iclin.unidades`;

# AGENDAS
CREATE OR REPLACE TABLE `{project_id}.vendas.iclin_agendas_gold`
AS SELECT * FROM `{project_id}.iclin.agendas`;

# CONVENIOS
CREATE OR REPLACE TABLE `{project_id}.vendas.iclin_convenios_gold`
AS SELECT * FROM `{project_id}.iclin.convenios`;

# ATENDIMENTOS
CREATE OR REPLACE TABLE `{project_id}.vendas.iclin_atendimentos_gold`
AS SELECT * FROM `{project_id}.iclin.atendimentos`;

# ATENDIMENTO_SERVICOS
CREATE OR REPLACE TABLE `{project_id}.vendas.iclin_atendimento_servicos_gold`
AS SELECT * FROM `{project_id}.iclin.atendimento_servicos`;

# CLIENTES
CREATE OR REPLACE TABLE `{project_id}.vendas.iclin_clientes_gold`
AS SELECT * FROM `{project_id}.iclin.clientes`;
