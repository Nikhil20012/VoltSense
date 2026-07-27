{{ config(materialized='table') }}

select distinct
    {{ dbt_utils.generate_surrogate_key(['city', 'state', 'zip_code', 'grid_region', 'utility_zone']) }} as geo_key,
    city,
    state,
    zip_code,
    grid_region,
    utility_zone
from {{ ref('station_metadata') }}