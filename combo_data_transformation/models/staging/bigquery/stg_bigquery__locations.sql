with

renamed as (
    select
        id as location_id,
        name as location_name,
        account_id,
        created_at as location_created_at,
        updated_at as location_updated_at,
        address as location_address,
        preferences as location_preferences,
        city as location_city,
        zipcode as location_zipcode,
        collective_agreement_id,
        country as location_country,
        partner_id,
        archived,
        default_break_duration,
        payroll_software_identifier,
        enable_public_holiday_configuration,
        public_holiday_overtime_rate,
        public_holiday_paid_by_default,
        location_type,
        minimum_shift_duration_for_automatic_breaks_in_hours
    from {{ source('sources', 'locations') }}
)

select
    location_id,
    location_name,
    account_id,
    location_created_at,
    location_updated_at,
    location_address,
    location_preferences,
    location_city,
    location_zipcode,
    collective_agreement_id,
    location_country,
    partner_id,
    archived,
    default_break_duration,
    payroll_software_identifier,
    enable_public_holiday_configuration,
    public_holiday_overtime_rate,
    public_holiday_paid_by_default,
    location_type,
    minimum_shift_duration_for_automatic_breaks_in_hours
from renamed
