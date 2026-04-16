with

renamed as (
    select
        id as membership_id,
        user_id,
        account_id,
        active as is_active_membership,
        role as membership_role,
        created_at,
        updated_at,
        sort_index,
        preferences as membership_preferences,
        onboarded,
        can_manage_published_planning,
        can_invalidate_shift,
        can_remove_employee,
        can_set_custom_rest_value,
        lastname,
        street_address,
        enhanced_medical_examination,
        firstname
    from {{ source('sources', 'memberships') }}
)

select
    membership_id,
    user_id,
    account_id,
    is_active_membership,
    membership_role,
    created_at,
    updated_at,
    sort_index,
    membership_preferences,
    onboarded,
    can_manage_published_planning,
    can_invalidate_shift,
    can_remove_employee,
    can_set_custom_rest_value,
    lastname,
    street_address,
    enhanced_medical_examination,
    firstname
from renamed
