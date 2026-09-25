-- depends_on: none (reads raw CSV)

-- One row per date: diesel price and the fuel surcharge percentage in effect,
-- cleaned and typed from the raw fuel surcharge CSV.

with source as (
    select *
    from read_csv('fuel_surcharge.csv', header = true, all_varchar = true)
),

cleaned as (
    select
        try_cast(trim("date") as date)                           as rate_date,
        try_cast(trim(diesel_price_per_gal) as decimal(6, 3))    as diesel_price_per_gal,
        try_cast(trim(fuel_surcharge_pct) as decimal(5, 2))      as fuel_surcharge_pct
    from source
)

select *
from cleaned
where rate_date is not null
  and fuel_surcharge_pct >= 0
qualify row_number() over (partition by rate_date order by fuel_surcharge_pct desc) = 1
