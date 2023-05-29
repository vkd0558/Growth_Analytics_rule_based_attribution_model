
with
    conversions_data as (select * from {{ source("conversions", "conversions") }}),

    stg_conversions as (select * from conversions_data)

select *
from stg_conversions

