with

renamed as (
    select
        id,
        user_contract_id,
        timeoff_id,
        original_shift_id,
        rest_type,
        starts_at,
        ends_at,
        created_at,
        updated_at,
        partner_id,
        custom_value,
        account_id
    from {{ source('sources', 'rests') }}
),

select
    id,
    user_contract_id,
    timeoff_id,
    original_shift_id,
    rest_type,
    starts_at,
    ends_at,
    created_at,
    updated_at,
    partner_id,
    custom_value,
    account_id
from renamed