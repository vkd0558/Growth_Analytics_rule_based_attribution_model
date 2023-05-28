with source as {
    select * from {{source('sessions','sessions')}}
},
stg_sessions as {
    select * from source
}

select * from stg_sessions