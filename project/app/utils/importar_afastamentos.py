import pandas as pd
from sqlmodel import Session
from sqlalchemy import text
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

def importar_afastamentos_dataframe(df: pd.DataFrame, session: Session) -> int:
    colunas_necessarias = {
        "Id_SERVIDOR_PORTAL": "id_servidor",
        "ANO": "ano",
        "MES": "mes",
        "DATA_INICIO_AFASTAMENTO": "inicio_afastamento"
    }

    for col in colunas_necessarias:
        if col not in df.columns:
            raise ValueError(f"Coluna ausente no CSV: {col}")

    df = df[colunas_necessarias.keys()].rename(columns=colunas_necessarias)

    df = df.dropna(subset=["id_servidor", "ano", "mes"])
    df = df[df["id_servidor"].str.isnumeric()]
    df["id_servidor"] = df["id_servidor"].astype(int)
    df["ano"] = df["ano"].astype(int)
    df["mes"] = df["mes"].astype(int)

    def parse_data(valor):
        if pd.isna(valor):
            return None
        try:
            return datetime.strptime(valor.strip(), "%d/%m/%Y").date()
        except:
            return None

    df["inicio_afastamento"] = df["inicio_afastamento"].apply(parse_data)
    df["duracao_dias"] = 1

    registros = df.to_dict(orient="records")
    total_processados = 0
    chunk_size = 1000

    sql = """
        INSERT INTO afastamentos (
            id_servidor, ano, mes, inicio_afastamento, duracao_dias
        ) VALUES (
            :id_servidor, :ano, :mes, :inicio_afastamento, :duracao_dias
        )
        ON CONFLICT DO NOTHING;
    """

    for i in range(0, len(registros), chunk_size):
        chunk = registros[i:i + chunk_size]
        session.execute(text(sql), chunk)
        session.commit()
        logger.info(f"Lote {i // chunk_size + 1} importado: {len(chunk)} afastamentos.")
        total_processados += len(chunk)

    logger.info(f"Importação concluída: {total_processados} afastamentos processados.")
    return total_processados