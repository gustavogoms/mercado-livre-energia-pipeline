select
    distribuidora,
    uf,
    ano,
    tarifa_media_mwh,
    pld_medio_anual,
    spread_mwh,
    economia_pct,
    spread_mwh * 100   as economia_100mwh_mes,
    spread_mwh * 500   as economia_500mwh_mes,
    spread_mwh * 1000  as economia_1000mwh_mes,
    spread_mwh * 3000  as economia_3000mwh_mes
from {{ ref('int_pld_vs_tarifa') }}
where spread_mwh > 0
order by ano desc, economia_pct desc
