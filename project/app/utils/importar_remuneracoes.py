import pandas as pd
from sqlmodel import Session
from sqlalchemy import text
import logging

logger = logging.getLogger(__name__)

def importar_remuneracoes_dataframe(df: pd.DataFrame, session: Session) -> int:
    colunas_necessarias = {
        "Id_SERVIDOR_PORTAL": "id_servidor",
        "ANO": "ano",
        "MES": "mes",
        "REMUNERAÇÃO BÁSICA BRUTA (R$)": "remuneracao",
        "IRRF (R$)": "irrf",
        "PSS/RPGS (R$)": "pss_rpgs",
        "REMUNERAÇÃO APÓS DEDUÇÕES OBRIGATÓRIAS (R$)": "remuneracao_final"
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

    def to_float(val: str) -> float:
        try:
            return float(val.replace(".", "").replace(",", "."))
        except:
            return 0.0

    for campo in ["remuneracao", "irrf", "pss_rpgs", "remuneracao_final"]:
        df[campo] = df[campo].fillna("0,00").astype(str).apply(to_float)

    registros = df.to_dict(orient="records")
    total_processados = 0
    chunk_size = 1000

    sql = """
        INSERT INTO remuneracoes (
            id_servidor, ano, mes, remuneracao, irrf, pss_rpgs, remuneracao_final
        ) VALUES (
            :id_servidor, :ano, :mes, :remuneracao, :irrf, :pss_rpgs, :remuneracao_final
        )
        ON CONFLICT DO NOTHING;
    """

    for i in range(0, len(registros), chunk_size):
        chunk = registros[i:i + chunk_size]
        session.execute(text(sql), chunk)
        session.commit()
        logger.info(f"Lote {i // chunk_size + 1} importado: {len(chunk)} remunerações.")
        total_processados += len(chunk)

    logger.info(f"Importação concluída: {total_processados} remunerações processadas.")
    return total_processados