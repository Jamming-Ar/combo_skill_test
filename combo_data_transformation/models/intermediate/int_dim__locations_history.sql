with

locations_snapshot as (select * from {{ ref('snapshot__locations') }})

select
    location_id,
    location_name,
    account_id,
    is_archived,
    location_city,
    location_zipcode,
    location_country,
    cast(dbt_valid_from as date) as dbt_valid_from,
    cast(dbt_valid_to as date) as dbt_valid_to
from locations_snapshot
