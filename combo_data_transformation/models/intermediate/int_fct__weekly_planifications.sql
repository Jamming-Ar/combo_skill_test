with

shifts as (select * from {{ ref('stg_bigquery__shifts') }}),
rests as (select * from {{ ref('stg_bigquery__rests') }}),
contracts as (select * from {{ ref('int_dim__contracts') }}),

shifts_weekly as (
    select
        user_contract_id,
        date_trunc(date(shift_starts_at), week(monday)) as week_start
    from shifts
    where shift_starts_at is not null
),

rests_weekly as (
    select
        user_contract_id,
        date_trunc(date(rest_starts_at), week(monday)) as week_start
    from rests
    where rest_starts_at is not null
),

planifications_unioned as (
    select * from shifts_weekly
    union all
    select * from rests_weekly
),

planned_contract_weeks as (
    select distinct
        user_contract_id,
        week_start
    from planifications_unioned
)

select
    planned_contract_weeks.week_start,
    planned_contract_weeks.user_contract_id,
    contracts.membership_id,
    contracts.location_id,
    contracts.account_id,
    contracts.is_duplicate_active_contract
from planned_contract_weeks
inner join contracts
    on planned_contract_weeks.user_contract_id = contracts.user_contract_id
