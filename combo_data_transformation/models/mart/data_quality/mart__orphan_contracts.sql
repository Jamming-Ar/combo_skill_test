with

shifts as (select * from {{ ref('stg_bigquery__shifts') }}),
rests as (select * from {{ ref('stg_bigquery__rests') }}),
user_contracts as (select * from {{ ref('stg_bigquery__user_contracts') }}),
accounts as (select * from {{ ref('int_dim__accounts') }}),

orphan_shifts as (
    select
        shifts.account_id,
        count(distinct shifts.user_contract_id) as orphan_contracts_from_shifts,
        count(*) as orphan_shifts_count
    from shifts
    left join user_contracts
        on shifts.user_contract_id = user_contracts.user_contract_id
    where user_contracts.user_contract_id is null
    group by shifts.account_id
),

orphan_rests as (
    select
        rests.account_id,
        count(distinct rests.user_contract_id) as orphan_contracts_from_rests,
        count(*) as orphan_rests_count
    from rests
    left join user_contracts
        on rests.user_contract_id = user_contracts.user_contract_id
    where user_contracts.user_contract_id is null
    group by rests.account_id
),

all_accounts as (
    select account_id from orphan_shifts
    union distinct
    select account_id from orphan_rests
)

select
    all_accounts.account_id,
    accounts.account_name,
    coalesce(orphan_shifts.orphan_contracts_from_shifts, 0) as orphan_contracts_from_shifts,
    coalesce(orphan_shifts.orphan_shifts_count, 0) as orphan_shifts_count,
    coalesce(orphan_rests.orphan_contracts_from_rests, 0) as orphan_contracts_from_rests,
    coalesce(orphan_rests.orphan_rests_count, 0) as orphan_rests_count
from all_accounts
left join orphan_shifts on all_accounts.account_id = orphan_shifts.account_id
left join orphan_rests on all_accounts.account_id = orphan_rests.account_id
left join accounts on all_accounts.account_id = accounts.account_id
order by orphan_shifts_count desc
