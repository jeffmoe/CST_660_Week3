-- depends_on: none (reads raw CSV)

-- One row per origin/destination ZIP pair, cleaned and typed from the raw lanes CSV.

with source as (
    select *
    from read_csv('lanes.csv', header = true, all_varchar = true)
),

cleaned as (
    select
        upper(trim(lane_id))                                 as lane_id,
        lpad(trim(origin_zip), 5, '0')                       as origin_zip,
        trim(origin_city)                                    as origin_city,
        upper(trim(origin_state))                            as origin_state,
        lpad(trim(dest_zip), 5, '0')                         as dest_zip,
        trim(dest_city)                                      as dest_city,
        upper(trim(dest_state))                              as dest_state,
        try_cast(trim(distance_miles) as integer)            as distance_miles,
        try_cast(trim(base_rate_per_cwt) as decimal(10, 2))  as base_rate_per_cwt
    from source
)

select *
from cleaned
where lane_id is not null
  and origin_zip is not null
  and dest_zip is not null
qualify row_number() over (partition by origin_zip, dest_zip order by lane_id) = 1
