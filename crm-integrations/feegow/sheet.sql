# PACIENTES
CREATE OR REPLACE EXTERNAL TABLE {project_id}.feegow.pacientes
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/pacientes/pacientes.csv']);

# AGENDAMENTOS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.feegow.agendamentos
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/agendamentos/agendamentos.csv']);

# FINANCEIRO_CONTAS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.feegow.financeiro_contas
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/financeiro_contas/financeiro_contas.csv']);

# FINANCEIRO_FATURAS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.feegow.financeiro_faturas
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/financeiro_faturas/financeiro_faturas.csv']);

# FINANCEIRO_FORNECEDORES
CREATE OR REPLACE EXTERNAL TABLE {project_id}.feegow.financeiro_fornecedores
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/financeiro_fornecedores/financeiro_fornecedores.csv']);

# PROFISSIONAIS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.feegow.profissionais
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/profissionais/profissionais.csv']);

# PROCEDIMENTOS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.feegow.procedimentos
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/procedimentos/procedimentos.csv']);

# ESPECIALIDADES
CREATE OR REPLACE EXTERNAL TABLE {project_id}.feegow.especialidades
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/especialidades/especialidades.csv']);

# CONVENIOS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.feegow.convenios
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/convenios/convenios.csv']);

# UNIDADES
CREATE OR REPLACE EXTERNAL TABLE {project_id}.feegow.unidades
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/unidades/unidades.csv']);

-- GOLD
# PACIENTES
CREATE OR REPLACE TABLE `{project_id}.vendas.feegow_pacientes_gold`
AS SELECT * FROM `{project_id}.feegow.pacientes`;

# AGENDAMENTOS
CREATE OR REPLACE TABLE `{project_id}.vendas.feegow_agendamentos_gold`
AS SELECT * FROM `{project_id}.feegow.agendamentos`;

# FINANCEIRO_CONTAS
CREATE OR REPLACE TABLE `{project_id}.vendas.feegow_financeiro_contas_gold`
AS SELECT * FROM `{project_id}.feegow.financeiro_contas`;

# FINANCEIRO_FATURAS
CREATE OR REPLACE TABLE `{project_id}.vendas.feegow_financeiro_faturas_gold`
AS SELECT * FROM `{project_id}.feegow.financeiro_faturas`;

# FINANCEIRO_FORNECEDORES
CREATE OR REPLACE TABLE `{project_id}.vendas.feegow_financeiro_fornecedores_gold`
AS SELECT * FROM `{project_id}.feegow.financeiro_fornecedores`;

# PROFISSIONAIS
CREATE OR REPLACE TABLE `{project_id}.vendas.feegow_profissionais_gold`
AS SELECT * FROM `{project_id}.feegow.profissionais`;

# PROCEDIMENTOS
CREATE OR REPLACE TABLE `{project_id}.vendas.feegow_procedimentos_gold`
AS SELECT * FROM `{project_id}.feegow.procedimentos`;

# ESPECIALIDADES
CREATE OR REPLACE TABLE `{project_id}.vendas.feegow_especialidades_gold`
AS SELECT * FROM `{project_id}.feegow.especialidades`;

# CONVENIOS
CREATE OR REPLACE TABLE `{project_id}.vendas.feegow_convenios_gold`
AS SELECT * FROM `{project_id}.feegow.convenios`;

# UNIDADES
CREATE OR REPLACE TABLE `{project_id}.vendas.feegow_unidades_gold`
AS SELECT * FROM `{project_id}.feegow.unidades`;
