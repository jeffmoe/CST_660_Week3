-- depends_on: staging.stg_shipments, staging.stg_lanes, staging.stg_fuel_surcharge
-- partition_by: pickup_date

-- One row per shipment with its lane and its cost and revenue including fuel surcharge.
--
-- The surcharge rate is the one in effect on the pickup date. An ASOF join takes
-- the latest rate on or before that date, so a gap in the rate table (a weekend,
-- or a weekly published index) still finds a rate.
--
-- The percentage applies to linehaul on both sides: the carrier charges it on
-- cost, and it is passed through to the customer on revenue.

with shipments as (
    select * from staging.stg_shipments
    where pickup_date = getvariable('run_date')
),

lanes as (
    select * from staging.stg_lanes
),

fuel as (
    -- All loaded dates, not just run_date: the ASOF join may need an earlier rate.
    select * from staging.stg_fuel_surcharge
),

joined as (
    select
        s.shipment_id,
        l.lane_id,
        s.origin_zip,
        l.origin_city,
        l.origin_state,
        s.dest_zip,
        l.dest_city,
        l.dest_state,
        l.distance_miles,
        s.pickup_date,
        s.delivery_date,
        s.bill_received_date,
        s.weight_lbs,
        f.rate_date as fuel_rate_date,
        f.fuel_surcharge_pct,
        s.linehaul_cost,
        s.linehaul_revenue
    from shipments as s
    left join lanes as l
        on s.origin_zip = l.origin_zip
       and s.dest_zip = l.dest_zip
    asof left join fuel as f
        on s.pickup_date >= f.rate_date
),

-- Dividing a decimal gives a double in DuckDB, so cast back to exact money.
surcharged as (
    select
        *,
        cast(round(linehaul_cost * fuel_surcharge_pct / 100, 2) as decimal(12, 2))    as fuel_surcharge_cost,
        cast(round(linehaul_revenue * fuel_surcharge_pct / 100, 2) as decimal(12, 2)) as fuel_surcharge_revenue
    from joined
)

select
    *,
    linehaul_cost + fuel_surcharge_cost           as total_cost,
    linehaul_revenue + fuel_surcharge_revenue     as total_revenue
from surcharged
