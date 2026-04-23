with

renamed as (
    select
        id as account_id,
        name as account_name,
        created_at as account_created_at,
        time_zone,
        country as account_country
    from {{ source('sources', 'accounts') }}
)

select
    account_id,
    account_name,
    account_created_at,
    time_zone,
    account_country
from renamed
