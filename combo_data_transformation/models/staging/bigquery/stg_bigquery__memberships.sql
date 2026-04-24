with

renamed as (
    select
        id as membership_id,
        user_id,
        account_id,
        case lower(active)
            when 'true'  then true
            when 'yes'   then true
            when '1'     then true
            when 'false' then false
            when 'no'    then false
            when '0'     then false
            else null
        end as is_active_membership,
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
