-- products
CREATE OR REPLACE EXTERNAL TABLE `{project_id}.hotmart.hotmart_produtos`
OPTIONS (
    format = 'CSV',
    field_delimiter = ',',
    skip_leading_rows = 1,
    allow_quoted_newlines = true,
    uris = ['gs://{bucket_name}/products/products*.csv']
);

-- sales_history
CREATE OR REPLACE EXTERNAL TABLE `{project_id}.hotmart.hotmart_sales_history`
OPTIONS (
    format = 'CSV',
    field_delimiter = ',',
    skip_leading_rows = 1,
    allow_quoted_newlines = true,
    uris = ['gs://{bucket_name}/sales_history/sales_history*.csv']
);

-- sales_summary
CREATE OR REPLACE EXTERNAL TABLE `{project_id}.hotmart.hotmart_sales_summary`
(
    items STRING,
    page_info STRING
)
OPTIONS (
    format = 'CSV',
    field_delimiter = ',',
    skip_leading_rows = 1,
    allow_quoted_newlines = true,
    uris = ['gs://{bucket_name}/sales_summary/sales_summary*.csv']
);

-- sales_users
CREATE OR REPLACE EXTERNAL TABLE `{project_id}.hotmart.hotmart_sales_users`
OPTIONS (
    format = 'CSV',
    field_delimiter = ',',
    skip_leading_rows = 1,
    allow_quoted_newlines = true,
    uris = ['gs://{bucket_name}/sales_users/sales_users*.csv']
);

-- subscriptions_items
CREATE OR REPLACE EXTERNAL TABLE `{project_id}.hotmart.hotmart_subscriptions_items`
OPTIONS (
    format = 'CSV',
    field_delimiter = ',',
    skip_leading_rows = 1,
    allow_quoted_newlines = true,
    uris = ['gs://{bucket_name}/subscriptions_summary/subscriptions_items*.csv']
);

-- subscriptions_summary
CREATE OR REPLACE EXTERNAL TABLE `{project_id}.hotmart.hotmart_subscriptions_summary`
(
    items STRING,
    page_info STRING
)
OPTIONS (
    format = 'CSV',
    field_delimiter = ',',
    skip_leading_rows = 1,
    allow_quoted_newlines = true,
    uris = ['gs://{bucket_name}/subscriptions_summary/subscriptions_summary*.csv']
);