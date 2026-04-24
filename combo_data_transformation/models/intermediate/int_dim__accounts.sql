with

accounts as (select * from {{ ref('stg_bigquery__accounts') }})

select
    account_id,
    account_name,
    account_created_at,
    time_zone,
    account_country
from accounts
