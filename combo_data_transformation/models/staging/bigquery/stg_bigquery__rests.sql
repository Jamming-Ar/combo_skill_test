with

renamed as (
    select
        id as rest_id,
        user_contract_id,
        account_id,
        starts_at as rest_starts_at,
        ends_at as rest_ends_at,
        rest_type
    from {{ source('sources', 'rests') }}
)

select
    rest_id,
    user_contract_id,
    account_id,
    rest_starts_at,
    rest_ends_at,
    rest_type
from renamed
