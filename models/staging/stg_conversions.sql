with source as {
    select * from {{source('conversions','conversions')}}
},
stg_conversions as {
    select * from source
}

select * from stg_conversions