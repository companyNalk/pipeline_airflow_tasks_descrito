# TEAM
CREATE OR REPLACE EXTERNAL TABLE {project_id}.clickup.team
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/team/team.csv']);

# TEAM_SPACES
CREATE OR REPLACE EXTERNAL TABLE {project_id}.clickup.team_spaces
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/team_spaces/team_spaces.csv']);

# TEAM_FIELDS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.clickup.team_fields
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/team_fields/team_fields.csv']);

# TEAM_CUSTOM_ITEMS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.clickup.team_custom_items
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/team_custom_items/team_custom_items.csv']);

# SPACE_TAGS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.clickup.space_tags
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/space_tags/space_tags.csv']);

# SPACE_FOLDERS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.clickup.space_folders
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/space_folders/space_folders.csv']);

# SPACE_FIELDS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.clickup.space_fields
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/space_fields/space_fields.csv']);

# LIST_TASKS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.clickup.list_tasks
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/list_tasks/list_tasks.csv']);

# LIST_FIELDS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.clickup.list_fields
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/list_fields/list_fields.csv']);

# FOLDER_LISTS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.clickup.folder_lists
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/folder_lists/folder_lists.csv']);

# FOLDER_FIELDS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.clickup.folder_fields
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/folder_fields/folder_fields.csv']);

-- GOLD
# TEAM
CREATE OR REPLACE TABLE `{project_id}.vendas.clickup_team_gold`
AS
SELECT *
FROM `{project_id}.clickup.team`;

# TEAM_SPACES
CREATE OR REPLACE TABLE `{project_id}.vendas.clickup_team_spaces_gold`
AS
SELECT *
FROM `{project_id}.clickup.team_spaces`;

# TEAM_FIELDS
CREATE OR REPLACE TABLE `{project_id}.vendas.clickup_team_fields_gold`
AS
SELECT *
FROM `{project_id}.clickup.team_fields`;

# TEAM_CUSTOM_ITEMS
CREATE OR REPLACE TABLE `{project_id}.vendas.clickup_team_custom_items_gold`
AS
SELECT *
FROM `{project_id}.clickup.team_custom_items`;

# SPACE_TAGS
CREATE OR REPLACE TABLE `{project_id}.vendas.clickup_space_tags_gold`
AS
SELECT *
FROM `{project_id}.clickup.space_tags`;

# SPACE_FOLDERS
CREATE OR REPLACE TABLE `{project_id}.vendas.clickup_space_folders_gold`
AS
SELECT *
FROM `{project_id}.clickup.space_folders`;

# SPACE_FIELDS
CREATE OR REPLACE TABLE `{project_id}.vendas.clickup_space_fields_gold`
AS
SELECT *
FROM `{project_id}.clickup.space_fields`;

# LIST_TASKS
CREATE OR REPLACE TABLE `{project_id}.vendas.clickup_list_tasks_gold`
AS
SELECT *
FROM `{project_id}.clickup.list_tasks`;

# LIST_FIELDS
CREATE OR REPLACE TABLE `{project_id}.vendas.clickup_list_fields_gold`
AS
SELECT *
FROM `{project_id}.clickup.list_fields`;

# FOLDER_LISTS
CREATE OR REPLACE TABLE `{project_id}.vendas.clickup_folder_lists_gold`
AS
SELECT *
FROM `{project_id}.clickup.folder_lists`;

# FOLDER_FIELDS
CREATE OR REPLACE TABLE `{project_id}.vendas.clickup_folder_fields_gold`
AS
SELECT *
FROM `{project_id}.clickup.folder_fields`;