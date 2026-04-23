with

renamed as (
    select
        id as shift_id,
        user_contract_id,
        account_id,
        starts_at as shift_starts_at,
        ends_at as shift_ends_at,
        planification_type
    from {{ source('sources', 'shifts') }}
)

select
    shift_id,
    user_contract_id,
    account_id,
    shift_starts_at,
    shift_ends_at,
    planification_type
from renamed
