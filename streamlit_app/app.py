import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
from pathlib import Path

st.set_page_config(
    page_title="Mercado Livre de Energia",
    page_icon="⚡",
    layout="wide"
)

@st.cache_data
def gerar_pld():
    datas = pd.date_range(start="2019-01-01", end="2024-12-31", freq="W")
    submercados = ["SE/CO", "S", "NE", "N"]
    registros = []
    np.random.seed(42)
    for data in datas:
        pld_base = np.random.uniform(50, 600)
        for sub in submercados:
            v = np.random.uniform(0.9, 1.1)
            registros.append({
                "data_referencia": data,
                "submercado": sub,
                "pld_medio_mwh": round(pld_base * v, 2),
            })
    return pd.DataFrame(registros)

@st.cache_data
def gerar_tarifas():
    distribuidoras = [
        ("Light", "RJ"), ("Enel RJ", "RJ"), ("CEMIG", "MG"),
        ("COPEL", "PR"), ("CELESC", "SC"), ("CPFL", "SP"),
        ("Enel SP", "SP"), ("EDP SP", "SP"), ("COELBA", "BA"),
        ("CELPE", "PE"), ("COSERN", "RN"), ("ENERGISA PB", "PB"),
        ("CELG", "GO"), ("Energisa MT", "MT"), ("CEMAR", "MA"),
    ]
    anos = range(2019, 2025)
    registros = []
    np.random.seed(7)
    for ano in anos:
        for dist, uf in distribuidoras:
            tarifa = np.random.uniform(600, 1200)
            registros.append({
                "distribuidora": dist,
                "uf": uf,
                "ano": ano,
                "tarifa_mwh": round(tarifa, 2),
            })
    return pd.DataFrame(registros)

@st.cache_data
def gerar_migracao():
    distribuidoras = [
        ("Light", "RJ"), ("Enel RJ", "RJ"), ("CEMIG", "MG"),
        ("COPEL", "PR"), ("CELESC", "SC"), ("CPFL", "SP"),
        ("Enel SP", "SP"), ("EDP SP", "SP"), ("COELBA", "BA"),
        ("CELPE", "PE"), ("COSERN", "RN"), ("ENERGISA PB", "PB"),
        ("CELG", "GO"), ("Energisa MT", "MT"), ("CEMAR", "MA"),
    ]
    anos = range(2019, 2025)
    registros = []
    np.random.seed(13)
    for ano in anos:
        for dist, uf in distribuidoras:
            fator = 1 + (ano - 2019) * 0.18
            consumidores = int(np.random.uniform(50, 500) * fator)
            registros.append({
                "distribuidora": dist,
                "uf": uf,
                "ano": ano,
                "consumidores_livres": consumidores,
                "consumo_total_mwh": round(consumidores * np.random.uniform(800, 2500), 2),
                "pct_mercado_livre": round(np.random.uniform(0.5, 15) * fator, 2),
            })
    return pd.DataFrame(registros)

@st.cache_data
def gerar_comparativo():
    tarifas = gerar_tarifas()
    pld = gerar_pld()
    pld["ano"] = pld["data_referencia"].dt.year
    pld_anual = pld.groupby(["ano", "submercado"])["pld_medio_mwh"].mean().reset_index()
    pld_anual.columns = ["ano", "submercado", "pld_medio_anual"]

    def get_submercado(uf):
        if uf in ['SP','RJ','MG','ES','GO','MT','MS','DF']:
            return 'SE/CO'
        elif uf in ['RS','SC','PR']:
            return 'S'
        elif uf in ['BA','SE','AL','PE','PB','RN','CE','PI','MA']:
            return 'NE'
        return 'N'

    tarifas["submercado"] = tarifas["uf"].apply(get_submercado)
    df = tarifas.merge(pld_anual, on=["ano", "submercado"], how="left")
    df["spread_mwh"] = df["tarifa_mwh"] - df["pld_medio_anual"]
    df["economia_pct"] = (df["spread_mwh"] / df["tarifa_mwh"] * 100).round(2)
    df["classificacao_vantagem"] = df["economia_pct"].apply(lambda x:
        "Alta vantagem" if x > 20 else
        "Media vantagem" if x > 10 else
        "Baixa vantagem" if x > 0 else
        "Mercado livre desfavoravel"
    )
    return df

st.title("⚡ Monitor do Mercado Livre de Energia")
st.caption("Dados simulados com base em fontes públicas — CCEE, ANEEL e EPE")

aba1, aba2, aba3, aba4 = st.tabs([
    "Migração de Consumidores",
    "Comparativo de Tarifas",
    "Simulador de Economia",
    "Análise de Risco do PLD"
])

with aba1:
    st.subheader("Evolução da migração para o mercado livre")
    df = gerar_migracao()

    col1, col2, col3 = st.columns(3)
    ultimo_ano = df["ano"].max()
    df_ultimo = df[df["ano"] == ultimo_ano]
    col1.metric("Consumidores livres", f"{df_ultimo['consumidores_livres'].sum():,.0f}")
    col2.metric("Consumo total (MWh)", f"{df_ultimo['consumo_total_mwh'].sum():,.0f}")
    col3.metric("Ano de referência", str(ultimo_ano))

    fig = px.bar(
        df.groupby("ano")["consumidores_livres"].sum().reset_index(),
        x="ano", y="consumidores_livres",
        title="Total de consumidores no mercado livre por ano",
        labels={"ano": "Ano", "consumidores_livres": "Consumidores"},
        color_discrete_sequence=["#1f77b4"]
    )
    st.plotly_chart(fig, use_container_width=True)

    fig2 = px.line(
        df, x="ano", y="consumidores_livres",
        color="distribuidora",
        title="Evolução por distribuidora",
        labels={"ano": "Ano", "consumidores_livres": "Consumidores"}
    )
    st.plotly_chart(fig2, use_container_width=True)

with aba2:
    st.subheader("Comparativo de tarifas — mercado cativo vs livre")
    df = gerar_comparativo()

    anos = sorted(df["ano"].unique(), reverse=True)
    ano_sel = st.selectbox("Selecione o ano", anos)
    df_fil = df[df["ano"] == ano_sel]

    fig = px.bar(
        df_fil.sort_values("economia_pct", ascending=True),
        x="economia_pct", y="distribuidora",
        orientation="h",
        color="classificacao_vantagem",
        title=f"Economia potencial no mercado livre ({ano_sel})",
        labels={"economia_pct": "Economia (%)", "distribuidora": "Distribuidora"},
        color_discrete_map={
            "Alta vantagem": "#2ca02c",
            "Media vantagem": "#98df8a",
            "Baixa vantagem": "#ffbb78",
            "Mercado livre desfavoravel": "#d62728"
        }
    )
    st.plotly_chart(fig, use_container_width=True)

    st.dataframe(
        df_fil[["distribuidora", "uf", "tarifa_mwh", "pld_medio_anual", "spread_mwh", "economia_pct", "classificacao_vantagem"]],
        use_container_width=True
    )

with aba3:
    st.subheader("Simulador de economia no mercado livre")
    st.caption("Estime quanto sua empresa economizaria migrando para o mercado livre")

    df = gerar_comparativo()

    col1, col2 = st.columns(2)
    with col1:
        distribuidoras = sorted(df["distribuidora"].unique())
        dist_sel = st.selectbox("Sua distribuidora atual", distribuidoras)
    with col2:
        consumo = st.slider("Consumo médio mensal (MWh)", min_value=100, max_value=5000, value=500, step=50)

    anos = sorted(df["ano"].unique(), reverse=True)
    ano_sel = st.selectbox("Ano de referência", anos, key="ano_sim")
    df_fil = df[(df["distribuidora"] == dist_sel) & (df["ano"] == ano_sel)]

    if not df_fil.empty:
        spread = df_fil["spread_mwh"].values[0]
        economia_mensal = spread * consumo
        economia_anual = economia_mensal * 12

        col1, col2, col3 = st.columns(3)
        col1.metric("Spread (R$/MWh)", f"R$ {spread:,.2f}")
        col2.metric("Economia mensal estimada", f"R$ {economia_mensal:,.2f}")
        col3.metric("Economia anual estimada", f"R$ {economia_anual:,.2f}")

        fig = go.Figure(go.Indicator(
            mode="gauge+number+delta",
            value=df_fil["economia_pct"].values[0],
            title={"text": "Economia potencial (%)"},
            delta={"reference": 10},
            gauge={
                "axis": {"range": [0, 50]},
                "bar": {"color": "#2ca02c"},
                "steps": [
                    {"range": [0, 10], "color": "#ffbb78"},
                    {"range": [10, 20], "color": "#98df8a"},
                    {"range": [20, 50], "color": "#2ca02c"},
                ]
            }
        ))
        st.plotly_chart(fig, use_container_width=True)

with aba4:
    st.subheader("Análise de risco do PLD")
    st.caption("Volatilidade histórica do Preço de Liquidação das Diferenças")

    df = gerar_pld()
    submercados = sorted(df["submercado"].unique())
    sub_sel = st.multiselect("Submercados", submercados, default=submercados)
    df_fil = df[df["submercado"].isin(sub_sel)]

    fig = px.line(
        df_fil, x="data_referencia", y="pld_medio_mwh",
        color="submercado",
        title="Histórico do PLD por submercado (R$/MWh)",
        labels={"data_referencia": "Data", "pld_medio_mwh": "PLD médio (R$/MWh)"}
    )
    st.plotly_chart(fig, use_container_width=True)

    df_stats = df_fil.groupby("submercado")["pld_medio_mwh"].agg(["mean", "std", "min", "max"]).reset_index()
    df_stats.columns = ["Submercado", "Média", "Desvio Padrão", "Mínimo", "Máximo"]
    st.dataframe(df_stats.round(2), use_container_width=True)
