with

rests as (select * from {{ ref('stg_bigquery__rests') }}),
contracts as (select * from {{ ref('int_dim__contracts') }}),

rests_weekly as (
    select distinct
        user_contract_id,
        date_trunc(date(rest_starts_at), week(monday)) as week_start
    from rests
    where rest_starts_at is not null
)

select
    rests_weekly.week_start,
    rests_weekly.user_contract_id,
    contracts.membership_id,
    contracts.location_id,
    contracts.account_id,
    contracts.is_duplicate_active_contract,
    1 as planned_rest_count
from rests_weekly
inner join contracts
    on rests_weekly.user_contract_id = contracts.user_contract_id
