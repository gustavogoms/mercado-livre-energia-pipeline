import requests
import pandas as pd
from pathlib import Path
from datetime import datetime

RAW_DIR = Path(__file__).parent.parent / "data" / "raw"


def simular_tarifas_aneel() -> pd.DataFrame:
    """
    Gera dados simulados de tarifas por distribuidora.
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
    np.random.seed(7)

    for ano in anos:
        for dist, uf in distribuidoras:
            tarifa_base = np.random.uniform(600, 1200)
            registros.append({
                "distribuidora": dist,
                "uf": uf,
                "ano": ano,
                "vigencia_inicio": f"{ano}-01-01",
                "tarifa_mwh": round(tarifa_base, 2),
                "tarifa_tp_mwh": round(tarifa_base * 1.35, 2),
                "tarifa_hfp_mwh": round(tarifa_base * 0.85, 2),
                "classe": "B3",
                "subgrupo": "BT",
            })

    return pd.DataFrame(registros)


def extrair_tarifas_aneel() -> str:
    """
    Tenta buscar tarifas reais da API de dados abertos da ANEEL.
    Se falhar, usa dados simulados.
    Salva em parquet em data/raw/.
    """
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    data_hoje = datetime.today().strftime("%Y-%m-%d")
    out_path = RAW_DIR / f"tarifas_aneel_{data_hoje}.parquet"

    print(f"[aneel_extractor] Iniciando extração — {data_hoje}")

    try:
        url = "https://dadosabertos.aneel.gov.br/api/3/action/datastore_search"
        params = {
            "resource_id": "b1bd71e7-d0ad-1935-a76b-5f2d0e00fc2a",
            "limit": 5000,
        }
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()

        records = resp.json()["result"]["records"]
        df = pd.DataFrame(records)

        df = df.rename(columns={
            "DscEmpresa": "distribuidora",
            "SigUF": "uf",
            "DatInicioVigencia": "vigencia_inicio",
            "VlrTarifaAplicada": "tarifa_mwh",
        })

        df["tarifa_mwh"] = pd.to_numeric(
            df["tarifa_mwh"].astype(str).str.replace(",", "."),
            errors="coerce"
        )
        df["vigencia_inicio"] = pd.to_datetime(
            df["vigencia_inicio"], errors="coerce"
        ).dt.strftime("%Y-%m-%d")

        df = df.dropna(subset=["tarifa_mwh"])
        print(f"[aneel_extractor] Download real OK — {len(df)} linhas")

    except Exception as e:
        print(f"[aneel_extractor] Download falhou ({e}) — usando simulação")
        df = simular_tarifas_aneel()

    colunas_esperadas = ["distribuidora", "uf", "tarifa_mwh", "vigencia_inicio"]
    for col in colunas_esperadas:
        if col not in df.columns:
            raise ValueError(f"Coluna ausente: {col}")

    df.to_parquet(out_path, index=False)
    print(f"[aneel_extractor] Salvo em {out_path} — {len(df)} linhas")

    return str(out_path)


if __name__ == "__main__":
    caminho = extrair_tarifas_aneel()
    print(f"\nArquivo gerado: {caminho}")

    df = pd.read_parquet(caminho)
    print(f"Shape: {df.shape}")
    print(df.head())
