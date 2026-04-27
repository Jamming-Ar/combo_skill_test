with

weekly_shifts as (select * from {{ ref('int_fct__weekly_planned_shifts') }}),
weekly_rests as (select * from {{ ref('int_fct__weekly_planned_rests') }}),
locations_history as (select * from {{ ref('int_dim__locations_history') }}),

planifications_unioned as (
    select week_start
    from weekly_shifts
    union all
    select week_start
    from weekly_rests
),

all_weeks as (
    select distinct week_start
    from planifications_unioned
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
)

select
    week_start,
    account_id,
    count(distinct location_id) as billable_location_count
from location_weeks
group by week_start, account_id
