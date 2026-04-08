with source as (
    select *
    from read_parquet('/opt/airflow/data/raw/migracao_mercado_livre_*.parquet')
),

renamed as (
    select
        trim(distribuidora)                         as distribuidora,
        trim(uf)                                    as uf,
        cast(ano as integer)                        as ano,
        cast(consumidores_livres_count as integer)  as consumidores_livres,
        cast(consumo_total_mwh as double)           as consumo_total_mwh,
        cast(percentual_mercado_livre as double)    as pct_mercado_livre
    from source
    where consumidores_livres_count > 0
)

select * from renamed
