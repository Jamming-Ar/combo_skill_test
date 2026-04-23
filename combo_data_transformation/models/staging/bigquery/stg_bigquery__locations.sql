with

renamed as (
    select
        id as location_id,
        name as location_name,
        account_id,
        archived,
        city as location_city,
        zipcode as location_zipcode,
        country as location_country
    from {{ source('sources', 'locations') }}
)

select
    location_id,
    location_name,
    account_id,
    archived,
    location_city,
    location_zipcode,
    location_country
from renamed
