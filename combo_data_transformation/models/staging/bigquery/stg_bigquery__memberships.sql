with

renamed as (
    select
        id as membership_id,
        user_id,
        account_id,
        active as is_active_membership,
        role as membership_role,
        firstname,
        lastname
    from {{ source('sources', 'memberships') }}
)

select
    membership_id,
    user_id,
    account_id,
    is_active_membership,
    membership_role,
    firstname,
    lastname
from renamed
