with
    sessions_data as (select * from {{ source("sessions", "sessions") }}),

    stg_sessions as (select * from sessions_data)

select *
from stg_sessions 
