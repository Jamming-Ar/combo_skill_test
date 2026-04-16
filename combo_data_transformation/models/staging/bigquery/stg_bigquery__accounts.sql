with

renamed as (
    select
        id,
        name,
        created_at,
        updated_at,
        time_zone,
        default_language,
        city,
        street,
        zip,
        collective_agreement_id,
        cheat_sheet_hour_cut,
        social_tax,
        paid_breaks,
        show_real_and_planned_hours_on_planning,
        preferences,
        blocked,
        payroll_software_id,
        partner_id,
        country
    from {{ source('sources', 'accounts') }}
),

select
    id,
    name,
    created_at,
    updated_at,
    time_zone,
    default_language,
    city,
    street,
    zip,
    collective_agreement_id,
    cheat_sheet_hour_cut,
    social_tax,
    paid_breaks,
    show_real_and_planned_hours_on_planning,
    preferences,
    blocked,
    payroll_software_id,
    partner_id,
    country
from renamed