with

shifts as (select * from {{ ref('stg_bigquery__shifts') }}),
contracts as (select * from {{ ref('int_dim__contracts') }}),

shifts_weekly as (
    select distinct
        user_contract_id,
        date_trunc(date(shift_starts_at), week(monday)) as week_start
    from shifts
    where shift_starts_at is not null
)

select
    shifts_weekly.week_start,
    shifts_weekly.user_contract_id,
    contracts.membership_id,
    contracts.location_id,
    contracts.account_id,
    contracts.is_duplicate_active_contract
from shifts_weekly
inner join contracts
    on shifts_weekly.user_contract_id = contracts.user_contract_id
