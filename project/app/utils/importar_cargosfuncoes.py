import pandas as pd
from sqlmodel import Session
from sqlalchemy import text
import logging

logger = logging.getLogger(__name__)

BIGINT_MAX = 9223372036854775807
BIGINT_MIN = -9223372036854775808


def importar_cargosfuncoes_dataframe(df: pd.DataFrame, session: Session) -> int:
    colunas_necessarias = {
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

    def limpar_str(val):
        if not isinstance(val, str):
            return None
        val = val.strip()
        if val.lower() in ["sem informação", "sem informaç", ""]:
            return None
        return val

    def limpar_int(val):
        try:
            val = float(val)
            if not pd.notna(val) or val in [-1.0, 0.0]:
                return None
            val_int = int(val)
            return val_int if BIGINT_MIN <= val_int <= BIGINT_MAX else None
        except:
            return None

    df["classe_cargo"] = df["classe_cargo"].apply(limpar_str)
    df["referencia_cargo"] = df["referencia_cargo"].apply(limpar_int)
    df["padrao_cargo"] = df["padrao_cargo"].apply(limpar_int)
    df["nivel_cargo"] = df["nivel_cargo"].apply(limpar_int)
    df["funcao"] = df["funcao"].apply(limpar_str)
    df["descricao_cargo"] = df["descricao_cargo"].fillna("").astype(str).str.strip()
    df["nivel_funcao"] = df["nivel_funcao"].apply(limpar_int)

    df["chave_logica"] = df[[
        "classe_cargo", "referencia_cargo", "padrao_cargo",
        "nivel_cargo", "funcao", "descricao_cargo", "nivel_funcao"
    ]].astype(str).agg("|".join, axis=1)

    df = df.drop_duplicates(subset="chave_logica")
    registros = df.drop(columns="chave_logica").to_dict(orient="records")

    registros_sanitizados = []
    for r in registros:
        for k, v in r.items():
            if isinstance(v, float) and (pd.isna(v) or v == float("inf") or v == float("-inf")):
                r[k] = None
            elif isinstance(v, (int, float)) and not BIGINT_MIN <= int(v) <= BIGINT_MAX:
                r[k] = None
        registros_sanitizados.append(r)

    total_processados = 0
    chunk_size = 1000

    sql = """
        INSERT INTO cargofuncao (
            classe_cargo, referencia_cargo, padrao_cargo,
            nivel_cargo, funcao, descricao_cargo, nivel_funcao
        ) VALUES (
            :classe_cargo, :referencia_cargo, :padrao_cargo,
            :nivel_cargo, :funcao, :descricao_cargo, :nivel_funcao
        )
        ON CONFLICT DO NOTHING;
    """

    for i in range(0, len(registros_sanitizados), chunk_size):
        chunk = registros_sanitizados[i:i + chunk_size]
        try:
            session.execute(text(sql), chunk)
            session.commit()
            logger.info(f"Lote {i // chunk_size + 1} importado: {len(chunk)} cargos/funções.")
            total_processados += len(chunk)
        except Exception as e:
            logger.exception(f"Erro ao importar lote {i // chunk_size + 1}: {e}")
            session.rollback()

    logger.info(f"Importação concluída: {total_processados} cargos/funções processados.")
    return total_processados