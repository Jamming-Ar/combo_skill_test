with

contratcs as (select * from {{ ref('int_dim__contracts') }})


select
    membership_id,
    firstname,
    lastname,
    account_id,
    location_id,
    count(*) as open_contract_count,
    array_agg(
        struct(
            user_contract_id, contract_start_date, contract_end_date, contract_type, contract_time
        )
        order by contract_start_date
    ) as detailed_open_contract
from contratcs
where contract_end_date is null
group by membership_id, firstname, lastname, account_id, location_id
having count(*) > 1
order by open_contract_count desc, lastname, firstname

-- Exemple contrat membership avec contrat ok : membership_id = 161572