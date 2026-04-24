with

renamed as (
    select
        id as user_contract_id,
        membership_id,
        location_id,
        account_id,
        {{ normalize_date('contract_start') }} 
            as contract_start,
        contract_end,
        contract_type,
        contract_time
    from {{ source('sources', 'user_contracts') }}
)

select
    user_contract_id,
    membership_id,
    location_id,
    account_id,
    contract_start,
    contract_end,
    contract_type,
    contract_time
from renamed
