with

contracts as (select * from {{ ref('stg_bigquery__user_contracts') }}),
memberships as (select * from {{ ref('stg_bigquery__memberships') }}),

contracts_enriched as (
    select
        contracts.user_contract_id,
        contracts.membership_id,
        contracts.location_id,
        contracts.account_id,
        contracts.contract_start_date,
        contracts.contract_end_date,
        contracts.contract_type,
        contracts.contract_time,
        memberships.user_id,
        memberships.firstname,
        memberships.lastname,
        ifnull(memberships.is_active_membership, false) as is_active_membership
    from contracts
    left join memberships
        on contracts.membership_id = memberships.membership_id
),

open_contract_counts as (
    select
        membership_id,
        location_id,
        count(*) as open_contract_count
    from contracts_enriched
    where contract_end_date is null
    group by membership_id, location_id
)

select
    contracts_enriched.user_contract_id,
    contracts_enriched.membership_id,
    contracts_enriched.location_id,
    contracts_enriched.account_id,
    contracts_enriched.contract_start_date,
    contracts_enriched.contract_end_date,
    contracts_enriched.contract_type,
    contracts_enriched.contract_time,
    contracts_enriched.user_id,
    contracts_enriched.firstname,
    contracts_enriched.lastname,
    contracts_enriched.is_active_membership,
    (
        contracts_enriched.contract_end_date is null
        and coalesce(open_contract_counts.open_contract_count, 1) > 1
    ) as is_duplicate_active_contract
from contracts_enriched
left join open_contract_counts
    on
        contracts_enriched.membership_id = open_contract_counts.membership_id
        and contracts_enriched.location_id = open_contract_counts.location_id
