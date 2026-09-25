-- depends_on: none (reads raw CSV)

-- One row per shipment, cleaned and typed from the raw shipments CSV.
--
-- Everything is read as text first so ZIPs like 07105 keep their leading zero,
-- then cast explicitly. Rows missing keys or with impossible values are dropped.
--
-- Deduplication: the feed contains exact duplicate rows and re-sent bills
-- (same shipment_id, later bill_received_date, corrected amounts). The most
-- recently received bill wins.

with source as (
    select *
    from read_csv('shipments.csv', header = true, all_varchar = true)
),

cleaned as (
    select
        upper(trim(shipment_id))                             as shipment_id,
        try_cast(trim(pickup_date) as date)                  as pickup_date,
        try_cast(trim(delivery_date) as date)                as delivery_date,
        try_cast(trim(bill_received_date) as date)           as bill_received_date,
        lpad(trim(origin_zip), 5, '0')                       as origin_zip,
        lpad(trim(dest_zip), 5, '0')                         as dest_zip,
        try_cast(trim(weight_lbs) as integer)                as weight_lbs,
        try_cast(trim(linehaul_revenue) as decimal(12, 2))   as linehaul_revenue,
        try_cast(trim(cost) as decimal(12, 2))               as linehaul_cost
    from source
)

select *
from cleaned
where shipment_id is not null
  and pickup_date is not null
  and delivery_date >= pickup_date
  and weight_lbs > 0
  and linehaul_revenue >= 0
  and linehaul_cost >= 0
qualify row_number() over (
    partition by shipment_id
    order by bill_received_date desc nulls last, linehaul_revenue desc, linehaul_cost desc
) = 1
