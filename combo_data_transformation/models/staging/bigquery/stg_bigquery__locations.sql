with

renamed as (
    select
        id as location_id,
        name as location_name,
        account_id,
        ifnull(case lower(archived)
            when 'true'  then true
            when 'yes'   then true
            when '1'     then true
            when 'false' then false
            when 'no'    then false
            when '0'     then false
            else null
        end, false) as is_archived,
        city as location_city,
        zipcode as location_zipcode,
        country as location_country
    from {{ source('sources', 'locations') }}
)

select
    location_id,
    location_name,
    account_id,
    is_archived,
    location_city,
    location_zipcode,
    location_country
from renamed
