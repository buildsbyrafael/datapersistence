from fastapi import APIRouter, UploadFile, File, HTTPException, status, Depends, Query
from sqlmodel import Session
from typing import List, Optional
from io import StringIO
import pandas as pd
import logging

from app.core.database import get_session
from app.schemas.remuneracao import RemuneracaoCreate, RemuneracaoRead
from app.utils.importar_remuneracoes import importar_remuneracoes_dataframe
from app.crud.remuneracao import (
    criar_remuneracao,
    criar_remuneracao_from_dict,
    buscar_remuneracao_por_id,
    listar_todas_remuneracoes,
    buscar_remuneracoes_filtradas,
    atualizar_remuneracao,
    atualizar_remuneracao_completa,
    deletar_remuneracao,
    deletar_remuneracoes_por_servidor,
    deletar_remuneracoes_por_periodo,
    obter_estatisticas_remuneracao,
    verificar_duplicata,
    buscar_historico_servidor,
)

router = APIRouter(prefix="/remuneracoes", tags=["Remunerações"])
logger = logging.getLogger(__name__)

@router.put("/importar", summary="Importar CSV de remunerações")
async def importar_remuneracoes_csv_api(
    arquivo: UploadFile = File(...)
):
    if not arquivo.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="O arquivo deve ser um CSV.")

    try:
        conteudo = await arquivo.read()
        df = pd.read_csv(StringIO(conteudo.decode("latin1")), dtype=str, sep=";")

        with next(get_session()) as session:
            total = importar_remuneracoes_dataframe(df, session)

        return {"mensagem": f"{total} remunerações importadas com sucesso!"}

    except Exception as e:
        logger.exception("Erro ao importar remunerações via API")
        raise HTTPException(status_code=500, detail=f"Erro interno: {e}")


@router.post(
    "/",
    response_model=RemuneracaoRead,
    status_code=status.HTTP_201_CREATED,
    summary="Criar nova remuneração"
)
def criar_remuneracao_endpoint(
    remuneracao: RemuneracaoCreate,
    session: Session = Depends(get_session)
):
    try:
        return criar_remuneracao_from_dict(session, dados=remuneracao.dict())
    except Exception:
        logger.exception("Erro ao criar remuneração")
        raise HTTPException(status_code=500, detail="Erro ao criar remuneração")

@router.get(
    "/todas",
    response_model=List[RemuneracaoRead],
    summary="Listar todas as remunerações"
)
def listar_todas_remuneracoes_endpoint(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    session: Session = Depends(get_session)
):
    try:
        return listar_todas_remuneracoes(session, limit=limit, offset=offset)
    except Exception:
        logger.exception("Erro ao listar todas as remunerações")
        raise HTTPException(status_code=500, detail="Erro ao listar remunerações")


# GET /remuneracoes/filtrar      → listagem com filtros
@router.get(
    "/filtrar",
    response_model=List[RemuneracaoRead],
    summary="Buscar remunerações com filtros"
)
def filtrar_remuneracoes_endpoint(
    ano: int = Query(..., ge=2000, le=2100),
    mes: int = Query(..., ge=1, le=12),
    remuneracao_min: Optional[float] = Query(None),
    remuneracao_max: Optional[float] = Query(None),
    irrf_min: Optional[float] = Query(None),
    irrf_max: Optional[float] = Query(None),
    pss_rpgs_min: Optional[float] = Query(None),
    pss_rpgs_max: Optional[float] = Query(None),
    remuneracao_final_min: Optional[float] = Query(None),
    remuneracao_final_max: Optional[float] = Query(None),
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    session: Session = Depends(get_session),
):
    try:
        return buscar_remuneracoes_filtradas(
            session=session,
            ano=ano,
            mes=mes,
            remuneracao_min=remuneracao_min,
            remuneracao_max=remuneracao_max,
            irrf_min=irrf_min,
            irrf_max=irrf_max,
            pss_rpgs_min=pss_rpgs_min,
            pss_rpgs_max=pss_rpgs_max,
            remuneracao_final_min=remuneracao_final_min,
            remuneracao_final_max=remuneracao_final_max,
            limit=limit,
            offset=offset,
        )
    except Exception:
        logger.exception("Erro ao filtrar remunerações")
        raise HTTPException(status_code=500, detail="Erro ao filtrar remunerações")
    
@router.get(
    "/duplicata",
    response_model=RemuneracaoRead,
    summary="Verificar duplicata de remuneração"
)
def duplicata_endpoint(
    id_servidor: int = Query(...),
    ano: int = Query(..., ge=2000, le=2100),
    mes: int = Query(..., ge=1, le=12),
    session: Session = Depends(get_session)
):
    dup = verificar_duplicata(session, id_servidor, ano, mes)
    if not dup:
        raise HTTPException(status_code=404, detail="Nenhuma remuneração duplicada encontrada")
    return dup


# GET /remuneracoes/historico/{id_servidor}
@router.get(
    "/historico/{id_servidor}",
    response_model=List[RemuneracaoRead],
    summary="Histórico de remunerações de um servidor"
)
def historico_endpoint(
    id_servidor: int,
    ano_inicio: Optional[int] = Query(None, ge=2000, le=2100),
    ano_fim: Optional[int] = Query(None, ge=2000, le=2100),
    ordenar_por_data: bool = Query(True),
    session: Session = Depends(get_session)
):
    try:
        return buscar_historico_servidor(
            session,
            id_servidor,
            ano_inicio=ano_inicio,
            ano_fim=ano_fim,
            ordenar_por_data=ordenar_por_data
        )
    except Exception:
        logger.exception("Erro ao buscar histórico de servidor")
        raise HTTPException(status_code=500, detail="Erro ao buscar histórico de servidor")


# GET /remuneracoes/{id}          → buscar por ID
@router.get(
    "/{id_remuneracao}",
    response_model=RemuneracaoRead,
    summary="Buscar remuneração por ID"
)
def buscar_remuneracao_endpoint(
    id_remuneracao: int,
    session: Session = Depends(get_session)
):
    remun = buscar_remuneracao_por_id(session, id_remuneracao)
    if not remun:
        raise HTTPException(status_code=404, detail="Remuneração não encontrada")
    return remun

@router.put(
    "/{id_remuneracao}",
    response_model=RemuneracaoRead,
    summary="Atualizar remuneração existente"
)
def atualizar_remuneracao_endpoint(
    id_remuneracao: int,
    dados: RemuneracaoCreate,
    session: Session = Depends(get_session)
):
    payload = dados.dict(exclude_unset=True)
    updated = atualizar_remuneracao(session, id_remuneracao, payload)
    if not updated:
        raise HTTPException(status_code=404, detail="Remuneração não encontrada")
    return updated


# DELETE /remuneracoes/{id}       → deletar uma
@router.delete(
    "/{id_remuneracao}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Deletar remuneração por ID"
)
def deletar_remuneracao_endpoint(
    id_remuneracao: int,
    session: Session = Depends(get_session)
):
    ok = deletar_remuneracao(session, id_remuneracao)
    if not ok:
        raise HTTPException(status_code=404, detail="Remuneração não encontrada")


# DELETE /remuneracoes/servidor/{id_servidor}
@router.delete(
    "/servidor/{id_servidor}",
    summary="Deletar remunerações por servidor"
)
def deletar_por_servidor_endpoint(
    id_servidor: int,
    ano: Optional[int] = Query(None, ge=2000, le=2100),
    mes: Optional[int] = Query(None, ge=1, le=12),
    session: Session = Depends(get_session)
):
    count = deletar_remuneracoes_por_servidor(session, id_servidor, ano=ano, mes=mes)
    return {"removidas": count}


# DELETE /remuneracoes/periodo    → deletar por período
@router.delete(
    "/periodo",
    summary="Deletar remunerações por período"
)
def deletar_por_periodo_endpoint(
    ano: int = Query(..., ge=2000, le=2100),
    mes: Optional[int] = Query(None, ge=1, le=12),
    session: Session = Depends(get_session)
):
    count = deletar_remuneracoes_por_periodo(session, ano=ano, mes=mes)
    return {"removidas": count}

