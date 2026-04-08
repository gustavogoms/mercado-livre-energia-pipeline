with source as (
    select *
    from read_parquet('/opt/airflow/data/raw/tarifas_aneel_*.parquet')
),

renamed as (
    select
        trim(distribuidora)             as distribuidora,
        trim(uf)                        as uf,
        cast(ano as integer)            as ano,
        cast(vigencia_inicio as date)   as vigencia_inicio,
        cast(tarifa_mwh as double)      as tarifa_mwh,
        cast(tarifa_tp_mwh as double)   as tarifa_tp_mwh,
        cast(tarifa_hfp_mwh as double)  as tarifa_hfp_mwh
    from source
    where tarifa_mwh > 0
)

select * from renamed
