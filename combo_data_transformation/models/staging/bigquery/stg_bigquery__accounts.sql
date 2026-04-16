with

renamed as (
    select
        id as account_id,
        name as account_name,
        created_at,
        updated_at,
        time_zone,
        default_language as account_default_language,
        city as account_city,
        street,
        zip as account_zip,
        collective_agreement_id,
        cheat_sheet_hour_cut,
        social_tax,
        paid_breaks,
        show_real_and_planned_hours_on_planning,
        preferences as account_preferences,
        blocked as is_blocked,
        payroll_software_id,
        partner_id,
        country as account_country
    from {{ source('sources', 'accounts') }}
)

select
    account_id,
    account_name,
    created_at,
    updated_at,
    time_zone,
    account_default_language,
    account_city,
    street,
    account_zip,
    collective_agreement_id,
    cheat_sheet_hour_cut,
    social_tax,
    paid_breaks,
    show_real_and_planned_hours_on_planning,
    account_preferences,
    is_blocked,
    payroll_software_id,
    partner_id,
    account_country
from renamed