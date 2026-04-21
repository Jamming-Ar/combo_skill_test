with

renamed as (
    select
        id as shift_id,
        user_contract_id,
        label_id,
        breakduration as breakduration_in_minutes,
        starts_at as shift_starts_at,
        ends_at as shift_ends_at,
        real_breakduration as real_breakduration_in_minutes,
        real_starts_at,
        real_ends_at,
        did_not_show,
        validated_at,
        validator_id,
        planned_hours_last_edited_at,
        planned_hours_last_edited_by_id,
        created_at as shift_created_at,
        updated_at as shift_updated_at,
        locked_at,
        partner_id,
        account_id,
        planification_type
    from {{ source('sources', 'shifts') }}
)

select
    shift_id,
    user_contract_id,
    label_id,
    breakduration_in_minutes,
    shift_starts_at,
    shift_ends_at,
    real_breakduration_in_minutes,
    real_starts_at,
    real_ends_at,
    did_not_show,
    validated_at,
    validator_id,
    planned_hours_last_edited_at,
    planned_hours_last_edited_by_id,
    shift_created_at,
    shift_updated_at,
    locked_at,
    partner_id,
    account_id,
    planification_type
from renamed
