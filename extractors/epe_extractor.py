import requests
import pandas as pd
from pathlib import Path
from datetime import datetime

RAW_DIR = Path(__file__).parent.parent / "data" / "raw"


def simular_migracao_mercado_livre() -> pd.DataFrame:
    """
    Gera dados simulados de migração de consumidores
    para o mercado livre por distribuidora e ano.
    """
    import numpy as np

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
            fator_crescimento = 1 + (ano - 2019) * 0.18
            consumidores = int(np.random.uniform(50, 500) * fator_crescimento)
            consumo_mwh = consumidores * np.random.uniform(800, 2500)

            registros.append({
                "distribuidora": dist,
                "uf": uf,
                "ano": ano,
                "consumidores_livres_count": consumidores,
                "consumo_total_mwh": round(consumo_mwh, 2),
                "percentual_mercado_livre": round(
                    np.random.uniform(0.5, 15) * fator_crescimento, 2
                ),
            })

    return pd.DataFrame(registros)


def extrair_migracao_mercado_livre() -> str:
    """
    Extrai dados de migração para o mercado livre.
    Usa simulação enquanto a fonte real não está disponível.
    Salva em parquet em data/raw/.
    """
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    data_hoje = datetime.today().strftime("%Y-%m-%d")
    out_path = RAW_DIR / f"migracao_mercado_livre_{data_hoje}.parquet"

    print(f"[epe_extractor] Iniciando extração — {data_hoje}")

    try:
        url = (
            "https://www.ccee.org.br/download/downloadArquivo"
            "?identificador=consumidores_livres_historico"
        )
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()

        df = pd.read_csv(
            pd.io.common.BytesIO(resp.content),
            sep=";",
            encoding="latin1",
        )
        print(f"[epe_extractor] Download real OK — {len(df)} linhas")

    except Exception as e:
        print(f"[epe_extractor] Download falhou ({e}) — usando simulação")
        df = simular_migracao_mercado_livre()

    colunas_esperadas = [
        "distribuidora", "uf", "ano", "consumidores_livres_count"
    ]
    for col in colunas_esperadas:
        if col not in df.columns:
            raise ValueError(f"Coluna ausente: {col}")

    df.to_parquet(out_path, index=False)
    print(f"[epe_extractor] Salvo em {out_path} — {len(df)} linhas")

    return str(out_path)


if __name__ == "__main__":
    caminho = extrair_migracao_mercado_livre()
    print(f"\nArquivo gerado: {caminho}")

    df = pd.read_parquet(caminho)
    print(f"Shape: {df.shape}")
    print(df.head())
