with source as (
    select *
    from read_parquet('/opt/airflow/data/raw/pld_ccee_*.parquet')
),

renamed as (
    select
        cast(data_referencia as date)   as data_referencia,
        trim(submercado)                as submercado,
        cast(pld_medio_mwh as double)   as pld_medio_mwh,
        cast(pld_minimo_mwh as double)  as pld_minimo_mwh,
        cast(pld_maximo_mwh as double)  as pld_maximo_mwh
    from source
    where pld_medio_mwh > 0
)

select * from renamed
