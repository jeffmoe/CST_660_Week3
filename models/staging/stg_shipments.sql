-- depends_on: none (reads raw CSV)
-- partition_by: pickup_date

-- One row per shipment, cleaned and typed from the raw shipments CSV.
--
-- Everything is read as text first so ZIPs like 07105 keep their leading zero,
-- then cast explicitly.
--
-- Late-arriving bills: only bills received on or before the as_of date are
-- visible, as they would have been on that day. Bills usually arrive days
-- after pickup, so a partition fills in as later batches reprocess it inside
-- their lookback window, and a re-sent bill replaces the original then. An
-- as_of of null (direct builds) sees every bill.
--
-- Deduplication: the feed contains exact duplicate rows and re-sent bills
-- (same shipment_id, corrected values). For each shipment_id the newest
-- visible version wins, ordered by
--   1. bill_received_date, latest first (a missing date never wins over a real one), then
--   2. source_line, latest first: the row's position in the feed file. The feed
--      appends in arrival order, so on a same-day tie the later row is the newer bill.
-- source_line is unique, so every shipment has exactly one winner, whatever
-- the physical row order.
--
-- The newest version is chosen across ALL pickup dates before any other
-- filter, so:
--   * a correction that moves pickup_date removes the shipment from its old
--     partition instead of leaving a copy there, and
--   * if the newest version is invalid (e.g. negative cost), the shipment is
--     dropped rather than falling back to an older, superseded version.

with source as (
    -- row_number() over () directly on the scan numbers rows in file order;
    -- test_dedup.py checks this against the actual line order.
    select *, row_number() over () as source_line
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
        try_cast(trim(cost) as decimal(12, 2))               as linehaul_cost,
        source_line
    from source
),

latest as (
    select *
    from cleaned
    where shipment_id is not null
      and (getvariable('as_of') is null or bill_received_date <= getvariable('as_of'))
    qualify row_number() over (
        partition by shipment_id
        order by bill_received_date desc nulls last, source_line desc
    ) = 1
)

select * exclude (source_line)
from latest
where pickup_date = getvariable('run_date')
  and delivery_date >= pickup_date
  and weight_lbs > 0
  and linehaul_revenue >= 0
  and linehaul_cost >= 0
