# USERS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.learn_words.users
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/users/users.csv']);

# COURSES
CREATE OR REPLACE EXTERNAL TABLE {project_id}.learn_words.courses
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/courses/courses.csv']);

# USER_SUBSCRIPTIONS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.learn_words.user_subscriptions
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/user_subscriptions/user_subscriptions.csv']);

# USER_PROGRESS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.learn_words.user_progress
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/user_progress/user_progress.csv']);

# USER_COURSES
CREATE OR REPLACE EXTERNAL TABLE {project_id}.learn_words.user_courses
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/user_courses/user_courses.csv']);

# COURSE_USERS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.learn_words.course_users
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/course_users/course_users.csv']);

# COURSE_ANALYTICS
CREATE OR REPLACE EXTERNAL TABLE {project_id}.learn_words.course_analytics
OPTIONS (
  format = 'CSV',
  field_delimiter=';',
  skip_leading_rows=1,
  allow_quoted_newlines=true,
  uris = ['gs://{bucket_name}/course_analytics/course_analytics.csv']);

-- GOLD
# USERS
CREATE OR REPLACE TABLE `{project_id}.vendas.learn_words_users_gold`
AS
SELECT *
FROM `{project_id}.learn_words.users`;

# COURSES
CREATE OR REPLACE TABLE `{project_id}.vendas.learn_words_courses_gold`
AS
SELECT *
FROM `{project_id}.learn_words.courses`;

# USER_SUBSCRIPTIONS
CREATE OR REPLACE TABLE `{project_id}.vendas.learn_words_user_subscriptions_gold`
AS
SELECT *
FROM `{project_id}.learn_words.user_subscriptions`;

# USER_PROGRESS
CREATE OR REPLACE TABLE `{project_id}.vendas.learn_words_user_progress_gold`
AS
SELECT *
FROM `{project_id}.learn_words.user_progress`;

# USER_COURSES
CREATE OR REPLACE TABLE `{project_id}.vendas.learn_words_user_courses_gold`
AS
SELECT *
FROM `{project_id}.learn_words.user_courses`;

# COURSE_USERS
CREATE OR REPLACE TABLE `{project_id}.vendas.learn_words_course_users_gold`
AS
SELECT *
FROM `{project_id}.learn_words.course_users`;

# COURSE_ANALYTICS
CREATE OR REPLACE TABLE `{project_id}.vendas.learn_words_course_analytics_gold`
AS
SELECT *
FROM `{project_id}.learn_words.course_analytics`;