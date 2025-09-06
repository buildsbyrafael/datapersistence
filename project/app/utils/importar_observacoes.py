import pandas as pd
from sqlmodel import Session
from sqlalchemy import text
import logging

logger = logging.getLogger(__name__)

def importar_observacoes_dataframe(df: pd.DataFrame, session: Session) -> int:
    colunas_necessarias = {
        "Id_SERVIDOR_PORTAL": "id_servidor",
        "ANO": "ano",
        "MES": "mes",
        "OBSERVACAO": "observacao"
    }

    for col in colunas_necessarias:
        if col not in df.columns:
            raise ValueError(f"Coluna ausente no CSV: {col}")

    df = df[colunas_necessarias.keys()].rename(columns=colunas_necessarias)

    df = df.dropna(subset=["id_servidor", "ano", "mes", "observacao"])
    df = df[df["id_servidor"].str.isnumeric()]
    df["id_servidor"] = df["id_servidor"].astype(int)
    df["ano"] = df["ano"].astype(int)
    df["mes"] = df["mes"].astype(int)
    df["observacao"] = df["observacao"].astype(str).str.strip()

    df["flag_teto"] = df["observacao"].str.upper().str.contains("ACIMA DO TETO")

    registros = df.to_dict(orient="records")
    total_processados = 0
    chunk_size = 1000

    sql = """
        INSERT INTO observacoes (
            id_servidor, ano, mes, observacao, flag_teto
        ) VALUES (
            :id_servidor, :ano, :mes, :observacao, :flag_teto
        )
        ON CONFLICT DO NOTHING;
    """

    for i in range(0, len(registros), chunk_size):
        chunk = registros[i:i + chunk_size]
        session.execute(text(sql), chunk)
        session.commit()
        logger.info(f"Lote {i // chunk_size + 1} importado: {len(chunk)} observações.")
        total_processados += len(chunk)

    logger.info(f"Importação concluída: {total_processados} observações processadas.")
    return total_processados