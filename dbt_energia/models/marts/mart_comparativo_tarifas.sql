select
    distribuidora,
    uf,
    ano,
    tarifa_media_mwh,
    pld_medio_anual,
    spread_mwh,
    economia_pct,
    case
        when economia_pct > 20 then 'Alta vantagem'
        when economia_pct > 10 then 'Media vantagem'
        when economia_pct > 0  then 'Baixa vantagem'
        else 'Mercado livre desfavoravel'
    end as classificacao_vantagem
from {{ ref('int_pld_vs_tarifa') }}
order by ano desc, economia_pct desc
