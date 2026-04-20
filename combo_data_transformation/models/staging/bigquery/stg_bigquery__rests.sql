with

renamed as (
    select
        id as rest_id,
        user_contract_id,
        timeoff_id,
        original_shift_id,
        rest_type,
        starts_at as rest_starts_at,
        ends_at as rest_ends_at,
        created_at as rest_created_at,
        updated_at as rest_updated_at,
        partner_id,
        custom_value,
        account_id
    from {{ source('sources', 'rests') }}
)

select
    rest_id,
    user_contract_id,
    timeoff_id,
    original_shift_id,
    rest_type,
    rest_starts_at,
    rest_ends_at,
    rest_created_at,
    rest_updated_at,
    partner_id,
    custom_value,
    account_id
from renamed
