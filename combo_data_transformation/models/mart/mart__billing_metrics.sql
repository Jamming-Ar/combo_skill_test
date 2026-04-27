with

-- weekly_shifts as (select * from {{ ref('int_fct__weekly_planned_shifts') }}),
-- weekly_rests as (select * from {{ ref('int_fct__weekly_planned_rests') }}),
accounts as (select * from {{ ref('int_dim__accounts') }}),
billable_locations_per_account as (
    select * from {{ ref('int_fct__weekly_billable_location_account') }}
),
billable_employees_per_account as (
    select * from {{ ref('int_fct__weekly_billable_employee_account') }}
)

-- planned_shifts_per_account as (
--     select
--         weekly_shifts.week_start,
--         weekly_shifts.account_id,
--         sum(weekly_shifts.planned_shift_count) as planned_shift_count
--     from weekly_shifts
--     inner join locations_history
--         on
--             weekly_shifts.location_id = locations_history.location_id
--             and (
--                 locations_history.dbt_valid_to is null
--                 or weekly_shifts.week_start < locations_history.dbt_valid_to
--             )
--             and locations_history.is_archived = false
--     group by weekly_shifts.week_start, weekly_shifts.account_id
-- ),

-- planned_rests_per_account as (
--     select
--         weekly_rests.week_start,
--         weekly_rests.account_id,
--         sum(weekly_rests.planned_rest_count) as planned_rest_count
--     from weekly_rests
--     inner join locations_history
--         on
--             weekly_rests.location_id = locations_history.location_id
--             and (
--                 locations_history.dbt_valid_to is null
--                 or weekly_rests.week_start < locations_history.dbt_valid_to
--             )
--             and locations_history.is_archived = false
--     group by weekly_rests.week_start, weekly_rests.account_id
-- )

select
    billable_locations_per_account.week_start,
    billable_locations_per_account.account_id,
    accounts.account_name,
    billable_locations_per_account.billable_location_count,
    coalesce(billable_employees_per_account.account_billable_employee_count, 0)
        as billable_employee_count,
-- coalesce(planned_shifts_per_account.planned_shift_count, 0) as planned_shift_count,
-- coalesce(planned_rests_per_account.planned_rest_count, 0) as planned_rest_count
from billable_locations_per_account
left join billable_employees_per_account
    on
        billable_locations_per_account.week_start = billable_employees_per_account.week_start
        and billable_locations_per_account.account_id = billable_employees_per_account.account_id
left join accounts
    on billable_locations_per_account.account_id = accounts.account_id
-- left join planned_shifts_per_account
--     on
--         billable_locations_per_account.week_start = planned_shifts_per_account.week_start
--         and billable_locations_per_account.account_id = planned_shifts_per_account.account_id
-- left join planned_rests_per_account
--     on
--         billable_locations_per_account.week_start = planned_rests_per_account.week_start
--         and billable_locations_per_account.account_id = planned_rests_per_account.account_id
order by billable_locations_per_account.week_start, billable_locations_per_account.account_id

