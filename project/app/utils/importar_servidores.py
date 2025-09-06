import pandas as pd
from sqlmodel import Session
from sqlalchemy import text
import logging

logger = logging.getLogger(__name__)

def importar_servidores_dataframe(df: pd.DataFrame, session: Session) -> int:
    colunas_necessarias = {
        "Id_SERVIDOR_PORTAL": "id_servidor",
        "NOME": "nome",
        "CPF": "cpf",
        "DESCRICAO_CARGO": "descr_cargo",
        "ORGSUP_EXERCICIO": "org_superior",
        "ORG_EXERCICIO": "org_exercicio",
        "REGIME_JURIDICO": "regime",
        "JORNADA_DE_TRABALHO": "jornada_trabalho"
    }

    for col in colunas_necessarias:
        if col not in df.columns:
            raise ValueError(f"Coluna ausente no CSV: {col}")

    df = df[colunas_necessarias.keys()].rename(columns=colunas_necessarias)

    campos_upper = ["org_superior", "org_exercicio", "regime", "jornada_trabalho"]
    for col in campos_upper:
        df[col] = df[col].fillna("").astype(str).str.strip().str.upper()

    df["nome"] = df["nome"].fillna("").astype(str).str.strip()
    df["cpf"] = df["cpf"].fillna("").astype(str).str.strip()
    df["descr_cargo"] = df["descr_cargo"].fillna("").astype(str).str.strip()

    df = df[df["id_servidor"].str.isnumeric()]
    df = df.drop_duplicates(subset="id_servidor")
    df["id_servidor"] = df["id_servidor"].astype(int)

    registros = df.to_dict(orient="records")
    total_processados = 0
    chunk_size = 1000

    sql = """
        INSERT INTO servidores (
            id_servidor, nome, cpf, descr_cargo,
            org_superior, org_exercicio, regime, jornada_trabalho
        ) VALUES (
            :id_servidor, :nome, :cpf, :descr_cargo,
            :org_superior, :org_exercicio, :regime, :jornada_trabalho
        )
        ON CONFLICT (id_servidor) DO NOTHING;
    """

    for i in range(0, len(registros), chunk_size):
        chunk = registros[i:i + chunk_size]
        session.execute(text(sql), chunk)
        session.commit()
        logger.info(f"Lote {i // chunk_size + 1} importado: {len(chunk)} servidores.")
        total_processados += len(chunk)

    logger.info(f"Importação concluída: {total_processados} servidores processados.")
    return total_processados