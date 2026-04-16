with

renamed as (
    select
        id as shift_id,
        user_contract_id,
        label_id,
        breakduration,
        starts_at,
        ends_at,
        real_breakduration,
        real_starts_at,
        real_ends_at,
        did_not_show,
        validated_at,
        validator_id,
        planned_hours_last_edited_at,
        planned_hours_last_edited_by_id,
        created_at,
        updated_at,
        locked_at,
        partner_id,
        account_id,
        planification_type
    from {{ source('sources', 'shifts') }}
),

select
    shift_id,
    user_contract_id,
    label_id,
    breakduration,
    starts_at,
    ends_at,
    real_breakduration,
    real_starts_at,
    real_ends_at,
    did_not_show,
    validated_at,
    validator_id,
    planned_hours_last_edited_at,
    planned_hours_last_edited_by_id,
    created_at,
    updated_at,
    locked_at,
    partner_id,
    account_id,
    planification_type
from renamed