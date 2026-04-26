with

weekly_location_employees as (select * from {{ ref('int_fct__weekly_billable_employee') }}),
accounts as (select * from {{ ref('int_dim__accounts') }}),

location_revenue as (
    select
        weekly_location_employees.week_start,
        weekly_location_employees.location_id,
        weekly_location_employees.account_id,
        weekly_location_employees.billable_employee_count,
        {{ calculate_location_monthly_price
            ('weekly_location_employees.billable_employee_count') }}                                                                         
            as monthly_location_price
    from weekly_location_employees
)

select
    location_revenue.week_start,
    location_revenue.account_id,
    accounts.account_name,
    sum(location_revenue.monthly_location_price) as expected_monthly_revenue
from location_revenue
left join accounts
    on location_revenue.account_id = accounts.account_id
group by
    location_revenue.week_start,
    location_revenue.account_id,
    accounts.account_name
order by location_revenue.week_start, location_revenue.account_id
