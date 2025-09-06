import pandas as pd
from sqlmodel import Session, select
from sqlalchemy import text
from datetime import datetime
import logging

from app.models.cargofuncao import CargoFuncao

logger = logging.getLogger(__name__)


def importar_funcaocargo_dataframe(df: pd.DataFrame, session: Session) -> int:
    colunas_necessarias = {
        "Id_SERVIDOR_PORTAL": "id_servidor",
        "DATA_INGRESSO_CARGOFUNCAO": "data_ingresso_funcao",
        "CLASSE_CARGO": "classe_cargo",
        "REFERENCIA_CARGO": "referencia_cargo",
        "PADRAO_CARGO": "padrao_cargo",
        "NIVEL_CARGO": "nivel_cargo",
        "FUNCAO": "funcao",
        "DESCRICAO_CARGO": "descricao_cargo",
        "NIVEL_FUNCAO": "nivel_funcao"
    }

    for col in colunas_necessarias:
        if col not in df.columns:
            raise ValueError(f"Coluna ausente no CSV: {col}")

    df = df[colunas_necessarias.keys()].rename(columns=colunas_necessarias)
    df = df.dropna(subset=["id_servidor", "descricao_cargo"])
    df = df[df["id_servidor"].astype(str).str.isnumeric()]
    df["id_servidor"] = df["id_servidor"].astype(int)

    def parse_data(val):
        if pd.isna(val):
            return None
        try:
            return datetime.strptime(val.strip(), "%d/%m/%Y").date()
        except:
            return None

    def limpar_valor(val, campo=None):
        if pd.isna(val):
            return None
        if isinstance(val, str):
            val = val.strip()
            if val.lower() in ["sem informação", "sem informaç", "", "-1"]:
                return None
            if campo in ["padrao_cargo", "referencia_cargo", "nivel_cargo", "nivel_funcao"]:
                if not val.isdigit():
                    return None
                return int(val)
            return val
        try:
            return int(val) if campo in ["padrao_cargo", "referencia_cargo", "nivel_cargo", "nivel_funcao"] else val
        except:
            return None

    df["data_ingresso_funcao"] = df["data_ingresso_funcao"].apply(parse_data)

    todos_cargos = session.exec(select(CargoFuncao)).all()
    mapa_cargos = {}

    for cargo in todos_cargos:
        chave = (
            cargo.classe_cargo,
            cargo.padrao_cargo,
            cargo.nivel_cargo,
            cargo.descricao_cargo
        )
        mapa_cargos[chave] = cargo.id_cargo_funcao

    registros = []
    for _, row in df.iterrows():
        valores = {
            "classe_cargo": limpar_valor(row["classe_cargo"], "classe_cargo"),
            "padrao_cargo": limpar_valor(row["padrao_cargo"], "padrao_cargo"),
            "nivel_cargo": limpar_valor(row["nivel_cargo"], "nivel_cargo"),
            "descricao_cargo": limpar_valor(row["descricao_cargo"], "descricao_cargo"),
        }

        chave = (
            valores["classe_cargo"],
            valores["padrao_cargo"],
            valores["nivel_cargo"],
            valores["descricao_cargo"]
        )

        id_cargo = mapa_cargos.get(chave)

        if id_cargo:
            registros.append({
                "id_servidor": row["id_servidor"],
                "id_cargo_funcao": id_cargo,
                "data_ingresso_funcao": row["data_ingresso_funcao"]
            })
        else:
            logger.warning(f"[SKIP] Cargo não encontrado para servidor {row['id_servidor']} → {valores['descricao_cargo']}")

    if not registros:
        logger.warning("Nenhum vínculo válido para importar.")
        return 0

    total_processados = 0
    chunk_size = 1000

    sql = """
        INSERT INTO funcao_cargo (
            id_servidor, id_cargo_funcao, data_ingresso_funcao
        ) VALUES (
            :id_servidor, :id_cargo_funcao, :data_ingresso_funcao
        )
        ON CONFLICT DO NOTHING;
    """

    for i in range(0, len(registros), chunk_size):
        chunk = registros[i:i + chunk_size]
        try:
            session.execute(text(sql), chunk)
            session.commit()
            logger.info(f"Lote {i // chunk_size + 1} importado: {len(chunk)} vínculos.")
            total_processados += len(chunk)
        except Exception as e:
            logger.exception(f"Erro ao importar lote {i // chunk_size + 1}: {e}")
            session.rollback()

    logger.info(f"Importação concluída: {total_processados} vínculos processados.")
    return total_processados