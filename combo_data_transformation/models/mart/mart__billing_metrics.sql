with

weekly_planifications as (select * from {{ ref('int_fct__weekly_planifications') }}),
locations as (select * from {{ ref('int_dim__locations') }}),
accounts as (select * from {{ ref('int_dim__accounts') }}),

all_weeks as (
    select distinct week_start
    from weekly_planifications
),

non_archived_locations as (
    select
        location_id,
        account_id
    from locations
    where is_archived = false
),

location_weeks as (
    select
        all_weeks.week_start,
        non_archived_locations.location_id,
        non_archived_locations.account_id
    from all_weeks
    cross join non_archived_locations
),

billable_locations_per_account as (
    select
        week_start,
        account_id,
        count(distinct location_id) as billable_location_count
    from location_weeks
    group by week_start, account_id
),

billable_employees_per_account as (
    select
        weekly_planifications.week_start,
        weekly_planifications.account_id,
        count(distinct weekly_planifications.membership_id) as billable_employee_count
    from weekly_planifications
    inner join locations
        on weekly_planifications.location_id = locations.location_id
        and locations.is_archived = false
    group by weekly_planifications.week_start, weekly_planifications.account_id
)

select
    billable_locations_per_account.week_start,
    billable_locations_per_account.account_id,
    accounts.account_name,
    billable_locations_per_account.billable_location_count,
    coalesce(billable_employees_per_account.billable_employee_count, 0) as billable_employee_count
from billable_locations_per_account
left join billable_employees_per_account
    on billable_locations_per_account.week_start = billable_employees_per_account.week_start
    and billable_locations_per_account.account_id = billable_employees_per_account.account_id
left join accounts
    on billable_locations_per_account.account_id = accounts.account_id
order by billable_locations_per_account.week_start, billable_locations_per_account.account_id
