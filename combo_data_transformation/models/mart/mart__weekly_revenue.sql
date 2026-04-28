with

weekly_location_employees as (
    select * from {{ ref('int_fct__weekly_billable_employee_location') }}
),
accounts as (select * from {{ ref('int_dim__accounts') }}),

location_revenue as (
    select
        weekly_location_employees.week_start,
        weekly_location_employees.location_id,
        weekly_location_employees.account_id,
        weekly_location_employees.location_billable_employee_count,
        {{ calculate_expected_location_revenue
            ('weekly_location_employees.location_billable_employee_count') }}                                                                         
            as expected_weekly_revenue
    from weekly_location_employees
)

select
    location_revenue.week_start,
    location_revenue.account_id,
    accounts.account_name,
    sum(location_revenue.expected_weekly_revenue) as expected_weekly_revenue
from location_revenue
left join accounts
    on location_revenue.account_id = accounts.account_id
group by
    location_revenue.week_start,
    location_revenue.account_id,
    accounts.account_name
order by location_revenue.week_start, location_revenue.account_id
