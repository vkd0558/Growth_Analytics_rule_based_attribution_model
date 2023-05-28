-- Define the source 'conversions'
-- Define the source 'sessions'
with
    source_conversions as (select * from {{ source("conversions", "conversions") }}),

    stg_conversions as (select * from source_conversions),

-- Select all columns from the stg_conversions table
select *
from stg_conversions
;
