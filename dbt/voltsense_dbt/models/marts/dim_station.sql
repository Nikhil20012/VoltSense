{{ config(materialized='table') }}

select
    station_id,
    station_name,
    latitude,
    longitude,
    city,
    state,
    zip_code,
    grid_region,
    utility_zone,
    max_capacity_kw,
    total_connectors,
    pricing_tier,
    station_type,
    install_date,
    '2026-01-01'::date as valid_from,
    '9999-12-31'::date as valid_to,
    true as is_current
from {{ ref('station_metadata') }}