with base as (
    select
        m.distribuidora,
        m.uf,
        m.ano,
        m.consumidores_livres,
        m.consumo_total_mwh,
        m.pct_mercado_livre,
        m.consumidores_livres
            - lag(m.consumidores_livres)
                over (partition by m.distribuidora order by m.ano)
            as crescimento_absoluto
    from {{ ref('stg_migracao') }} m
)

select
    *,
    round(
        crescimento_absoluto * 100.0
        / nullif(consumidores_livres - crescimento_absoluto, 0),
    2) as crescimento_pct
from base
order by ano desc, consumidores_livres desc
