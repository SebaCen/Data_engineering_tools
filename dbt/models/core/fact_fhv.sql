{{ config(materialized="table") }}

with
    fhv_fact as (
        select *, 'for hire vehicules' as service_type
        from {{ ref('stg_staging__fhv_trips_19') }}
    ),
    dim_zones as (select * from {{ ref("dim_zones") }} where borough != 'Unknown')
select
    fhv_fact.dispatching_base_num,
    fhv_fact.pickup_datetime,
    fhv_fact.dropoff_datetime,
    fhv_fact.pickup_locationid,
    fhv_fact.dropoff_locationid,
    fhv_fact.sr_flag,
    fhv_fact.affiliated_base_number,
    dropoff_zone.borough as dropoff_borough,
    dropoff_zone.zone as dropoff_zone,
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone
from fhv_fact
inner join dim_zones as pickup_zone
on fhv_fact.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv_fact.dropoff_locationid = dropoff_zone.locationid
