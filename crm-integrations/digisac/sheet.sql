# QUESTIONS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.digisac.questions
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/questions/questions.csv']);

# CONTACTS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.digisac.contacts
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/contacts/contacts.csv']);

# ANSWERS_OVERVIEW
CREATE OR REPLACE EXTERNAL TABLE {project_id}.digisac.answers_overview
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/answers_overview/answers_overview.csv']);

-- GOLD
# QUESTIONS
CREATE OR REPLACE TABLE `{project_id}.vendas.digisac_questions_gold`
AS
SELECT *
FROM `{project_id}.digisac.questions`;

# CONTACTS
CREATE OR REPLACE TABLE `{project_id}.vendas.digisac_contacts_gold`
AS
SELECT *
FROM `{project_id}.digisac.contacts`;

# ANSWERS_OVERVIEW
CREATE OR REPLACE TABLE `{project_id}.vendas.digisac_answers_overview_gold`
AS
SELECT *
FROM `{project_id}.digisac.answers_overview`;