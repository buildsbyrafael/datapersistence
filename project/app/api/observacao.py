from fastapi import APIRouter, UploadFile, File, HTTPException, status, Depends, Query
from sqlmodel import Session
from typing import List
from io import StringIO
import pandas as pd
import logging

from app.core.database import get_session
from app.schemas.observacao import ObservacaoRead
from app.utils.importar_observacoes import importar_observacoes_dataframe
from app.crud.observacao import buscar_por_mes_ano, listar_por_servidor, contar_observacoes_filtradas

router = APIRouter(prefix="/observacoes", tags=["Observações"])
logger = logging.getLogger(__name__)


@router.put("/importar", summary="Importar CSV de observações")
async def importar_observacoes_csv_api(
    arquivo: UploadFile = File(...)
):
    if not arquivo.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="O arquivo deve ser um CSV.")

    try:
        conteudo = await arquivo.read()
        df = pd.read_csv(StringIO(conteudo.decode("latin1")), dtype=str, sep=";")

        with next(get_session()) as session:
            total = importar_observacoes_dataframe(df, session)

        return {"mensagem": f"{total} observações importadas com sucesso!"}

    except Exception as e:
        logger.exception("Erro ao importar observações via API")
        raise HTTPException(status_code=500, detail=f"Erro interno: {e}")


@router.get("/", response_model=List[ObservacaoRead], summary="Listar observações por mês e ano")
def listar_observacoes(
    ano: int = Query(..., ge=2000, le=2100),
    mes: int = Query(..., ge=1, le=12),
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    session: Session = Depends(get_session)
):
    try:
        return buscar_por_mes_ano(session, ano=ano, mes=mes, limit=limit, offset=offset)
    except Exception as e:
        logger.exception("Erro ao listar observações")
        raise HTTPException(status_code=500, detail="Erro ao listar observações")


@router.get("/servidor/{id_servidor}", response_model=List[ObservacaoRead], summary="Listar observações por servidor")
def listar_observacoes_por_servidor(
    id_servidor: int,
    ano: int = Query(None, ge=2000, le=2100),
    mes: int = Query(None, ge=1, le=12),
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    session: Session = Depends(get_session)
):
    try:
        return listar_por_servidor(session, id_servidor=id_servidor, ano=ano, mes=mes, limit=limit, offset=offset)
    except Exception as e:
        logger.exception("Erro ao listar observações por servidor")
        raise HTTPException(status_code=500, detail="Erro ao listar observações do servidor")


@router.get("/contar", summary="Contar observações com base nos filtros de mês e ano")
def contar_observacoes_endpoint(
    ano: int = Query(..., ge=2000, le=2100),
    mes: int = Query(..., ge=1, le=12),
    session: Session = Depends(get_session)
):
    try:
        total = contar_observacoes_filtradas(session=session, ano=ano, mes=mes)
        return {"quantidade": total}
    except Exception as e:
        logger.exception("Erro ao contar observações")
        raise HTTPException(status_code=500, detail="Erro ao contar observações")