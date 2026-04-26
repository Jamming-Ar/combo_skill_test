with

weekly_shifts as (select * from {{ ref('int_fct__weekly_planned_shifts') }}),
weekly_rests as (select * from {{ ref('int_fct__weekly_planned_rests') }}),
locations_history as (select * from {{ ref('int_dim__locations_history') }}),

planifications_unioned as (
    select week_start, membership_id, location_id, account_id
    from weekly_shifts
    union all
    select week_start, membership_id, location_id, account_id
    from weekly_rests
),

weekly_planifications as (
    select distinct
        week_start,
        membership_id,
        location_id,
        account_id
    from planifications_unioned
),

all_weeks as (
    select distinct week_start
    from weekly_planifications
),

location_weeks as (
    select
        all_weeks.week_start,
        locations_history.location_id,
        locations_history.account_id
    from all_weeks
    inner join locations_history
        on (
            locations_history.dbt_valid_to is null
            or all_weeks.week_start < locations_history.dbt_valid_to
        )
        and locations_history.is_archived = false
),

employees_per_location as (
    select
        weekly_planifications.week_start,
        weekly_planifications.location_id,
        weekly_planifications.account_id,
        count(distinct weekly_planifications.membership_id) as billable_employee_count
    from weekly_planifications
    inner join locations_history
        on
            weekly_planifications.location_id = locations_history.location_id
            and (
                locations_history.dbt_valid_to is null
                or weekly_planifications.week_start < locations_history.dbt_valid_to
            )
            and locations_history.is_archived = false
    group by
        weekly_planifications.week_start,
        weekly_planifications.location_id,
        weekly_planifications.account_id
)

select
    location_weeks.week_start,
    location_weeks.location_id,
    location_weeks.account_id,
    coalesce(employees_per_location.billable_employee_count, 0) as billable_employee_count
from location_weeks
left join employees_per_location
    on
        location_weeks.week_start = employees_per_location.week_start
        and location_weeks.location_id = employees_per_location.location_id
