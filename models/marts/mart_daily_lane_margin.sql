-- depends_on: intermediate.int_shipment_lane_costs

-- Margin per lane per day, dated by pickup date (the same date the fuel
-- surcharge rate is taken from). Revenue and cost both include fuel surcharge.

with shipment_costs as (
    select * from intermediate.int_shipment_lane_costs
)

select
    pickup_date                                             as ship_date,
    lane_id,
    origin_zip,
    origin_city,
    origin_state,
    dest_zip,
    dest_city,
    dest_state,
    count(*)                                                as shipment_count,
    cast(sum(weight_lbs) as bigint)                         as total_weight_lbs,
    sum(linehaul_revenue)                                   as linehaul_revenue,
    sum(fuel_surcharge_revenue)                             as fuel_surcharge_revenue,
    sum(total_revenue)                                      as total_revenue,
    sum(linehaul_cost)                                      as linehaul_cost,
    sum(fuel_surcharge_cost)                                as fuel_surcharge_cost,
    sum(total_cost)                                         as total_cost,
    sum(total_revenue) - sum(total_cost)                    as margin,
    round(100 * (sum(total_revenue) - sum(total_cost))
          / nullif(sum(total_revenue), 0), 2)               as margin_pct
from shipment_costs
group by all
order by ship_date, lane_id
