import os
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime

RAW_DIR = Path(__file__).parent.parent / "data" / "raw"


def simular_pld() -> pd.DataFrame:
    """
    Gera dados simulados de PLD para desenvolvimento.
    Substitua pela fonte real quando disponível.
    """
    import numpy as np

    datas = pd.date_range(start="2019-01-01", end="2024-12-31", freq="W")
    submercados = ["SE/CO", "S", "NE", "N"]
    registros = []

    np.random.seed(42)
    for data in datas:
        pld_base = np.random.uniform(50, 600)
        for sub in submercados:
            variacao = np.random.uniform(0.9, 1.1)
            registros.append({
                "data_referencia": data.strftime("%Y-%m-%d"),
                "submercado": sub,
                "pld_medio_mwh": round(pld_base * variacao, 2),
                "pld_minimo_mwh": round(pld_base * variacao * 0.85, 2),
                "pld_maximo_mwh": round(pld_base * variacao * 1.15, 2),
            })

    return pd.DataFrame(registros)


def extrair_pld_ccee() -> str:
    """
    Tenta baixar o PLD real da CCEE.
    Se falhar, usa dados simulados.
    Salva em parquet em data/raw/.
    Retorna o caminho do arquivo salvo.
    """
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    data_hoje = datetime.today().strftime("%Y-%m-%d")
    out_path = RAW_DIR / f"pld_ccee_{data_hoje}.parquet"

    print(f"[ccee_extractor] Iniciando extração — {data_hoje}")

    try:
        url = (
            "https://www.ccee.org.br/download/downloadArquivo"
            "?identificador=pld_semanal_historico"
        )
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()

        df = pd.read_csv(
            pd.io.common.BytesIO(resp.content),
            sep=";",
            encoding="latin1",
            decimal=",",
        )

        df = df.rename(columns={
            df.columns[0]: "data_referencia",
            df.columns[1]: "submercado",
            df.columns[2]: "pld_medio_mwh",
        })

        df["data_referencia"] = pd.to_datetime(
            df["data_referencia"], dayfirst=True
        ).dt.strftime("%Y-%m-%d")

        df["pld_medio_mwh"] = pd.to_numeric(
            df["pld_medio_mwh"], errors="coerce"
        )

        df = df.dropna(subset=["pld_medio_mwh"])
        print(f"[ccee_extractor] Download real OK — {len(df)} linhas")

    except Exception as e:
        print(f"[ccee_extractor] Download falhou ({e}) — usando simulação")
        df = simular_pld()

    colunas_esperadas = ["data_referencia", "submercado", "pld_medio_mwh"]
    for col in colunas_esperadas:
        if col not in df.columns:
            raise ValueError(f"Coluna ausente no dataframe: {col}")

    df.to_parquet(out_path, index=False)
    print(f"[ccee_extractor] Salvo em {out_path} — {len(df)} linhas")

    return str(out_path)


if __name__ == "__main__":
    caminho = extrair_pld_ccee()
    print(f"\nArquivo gerado: {caminho}")

    df = pd.read_parquet(caminho)
    print(f"Shape: {df.shape}")
    print(df.head())
