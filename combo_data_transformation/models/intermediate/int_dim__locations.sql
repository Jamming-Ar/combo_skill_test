with

locations as (select * from {{ ref('stg_bigquery__locations') }})

select
    location_id,
    location_name,
    account_id,
    is_archived,
    location_city,
    location_zipcode,
    location_country
from locations
