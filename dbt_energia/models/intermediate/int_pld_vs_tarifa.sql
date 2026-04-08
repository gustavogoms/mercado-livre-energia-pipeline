with pld as (
    select
        year(data_referencia)               as ano,
        submercado,
        avg(pld_medio_mwh)                  as pld_medio_anual,
        min(pld_minimo_mwh)                 as pld_minimo_anual,
        max(pld_maximo_mwh)                 as pld_maximo_anual
    from {{ ref('stg_pld_ccee') }}
    group by 1, 2
),

tarifas as (
    select
        distribuidora,
        uf,
        ano,
        avg(tarifa_mwh) as tarifa_media_mwh
    from {{ ref('stg_tarifas_aneel') }}
    group by 1, 2, 3
),

joined as (
    select
        t.distribuidora,
        t.uf,
        t.ano,
        t.tarifa_media_mwh,
        p.pld_medio_anual,
        p.pld_minimo_anual,
        p.pld_maximo_anual,
        t.tarifa_media_mwh - p.pld_medio_anual as spread_mwh,
        round(
            (t.tarifa_media_mwh - p.pld_medio_anual)
            / t.tarifa_media_mwh * 100, 2
        ) as economia_pct
    from tarifas t
    left join pld p
        on t.ano = p.ano
        and p.submercado = case
            when t.uf in ('SP','RJ','MG','ES','GO','MT','MS','DF') then 'SE/CO'
            when t.uf in ('RS','SC','PR')                          then 'S'
            when t.uf in ('BA','SE','AL','PE','PB','RN','CE','PI','MA') then 'NE'
            else 'N'
        end
)

select * from joined
