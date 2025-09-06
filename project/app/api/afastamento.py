from fastapi import APIRouter, UploadFile, File, HTTPException, status, Depends, Query, Path
from sqlmodel import Session
from typing import List, Optional, Dict, Any
from io import StringIO
import pandas as pd
import logging

from app.core.database import get_session
from app.models.afastamento import AfastamentoUpdate
from app.schemas.afastamento import AfastamentoRead, AfastamentoCreate
from app.utils.importar_afastamentos import importar_afastamentos_dataframe
from app.crud.afastamento import (
    criar_afastamento,
    buscar_por_id,
    atualizar_afastamento,
    atualizar_afastamento_completo,
    deletar_afastamento,
    listar_todos,
    listar_por_servidor,
    buscar_por_mes_ano,
    buscar_por_periodo,
    buscar_por_duracao_minima,
    buscar_afastamentos_longos,
    contar_afastamentos_filtrados,
    contar_total_afastamentos,
    contar_afastamentos_por_servidor,
    deletar_afastamentos_por_servidor,
    obter_estatisticas_servidor
)

router = APIRouter(prefix="/afastamentos", tags=["Afastamentos"])
logger = logging.getLogger(__name__)

# IMPORT - Importar CSV
@router.put("/importar", summary="Importar CSV de afastamentos")
async def importar_afastamentos_csv_api(
    arquivo: UploadFile = File(...),
    session: Session = Depends(get_session)
):
    """Importa afastamentos de um arquivo CSV"""
    if not arquivo.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="O arquivo deve ser um CSV.")

    try:
        conteudo = await arquivo.read()
        df = pd.read_csv(StringIO(conteudo.decode("latin1")), dtype=str, sep=";")

        total = importar_afastamentos_dataframe(df, session)

        return {"mensagem": f"{total} afastamentos importados com sucesso!"}

    except Exception as e:
        logger.exception("Erro ao importar afastamentos via API")
        raise HTTPException(status_code=500, detail=f"Erro interno: {e}")


@router.post("/", response_model=AfastamentoRead, status_code=status.HTTP_201_CREATED, summary="Criar novo afastamento")
def criar_afastamento_endpoint(
    afastamento: AfastamentoCreate,
    session: Session = Depends(get_session)
):
    try:
        from app.models.afastamento import Afastamento
        novo_afastamento = Afastamento(**afastamento.dict())
        return criar_afastamento(session, novo_afastamento)
    except Exception as e:
        logger.exception("Erro ao criar afastamento")
        raise HTTPException(status_code=500, detail="Erro ao criar afastamento")

@router.get("/mes-ano", response_model=List[AfastamentoRead], summary="Listar afastamentos por mês e ano")
def listar_afastamentos_mes_ano(
    ano: int = Query(..., ge=2000, le=2100),
    mes: int = Query(..., ge=1, le=12),
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    session: Session = Depends(get_session)
):
    try:
        return buscar_por_mes_ano(session, ano=ano, mes=mes, limit=limit, offset=offset)
    except Exception as e:
        logger.exception("Erro ao listar afastamentos por mês/ano")
        raise HTTPException(status_code=500, detail="Erro ao listar afastamentos")


@router.get("/servidor/{id_servidor}", response_model=List[AfastamentoRead], summary="Listar afastamentos por servidor")
def listar_afastamentos_por_servidor(
    id_servidor: int = Path(..., gt=0),
    ano: Optional[int] = Query(None, ge=2000, le=2100),
    mes: Optional[int] = Query(None, ge=1, le=12),
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    session: Session = Depends(get_session)
):
    try:
        return listar_por_servidor(session, id_servidor=id_servidor, ano=ano, mes=mes, limit=limit, offset=offset)
    except Exception as e:
        logger.exception("Erro ao listar afastamentos por servidor")
        raise HTTPException(status_code=500, detail="Erro ao listar afastamentos do servidor")


@router.get("/periodo", response_model=List[AfastamentoRead], summary="Listar afastamentos por período")
def listar_afastamentos_periodo(
    ano_inicio: int = Query(..., ge=2000, le=2100),
    mes_inicio: int = Query(..., ge=1, le=12),
    ano_fim: int = Query(..., ge=2000, le=2100),
    mes_fim: int = Query(..., ge=1, le=12),
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    session: Session = Depends(get_session)
):
    try:
        return buscar_por_periodo(session, ano_inicio, mes_inicio, ano_fim, mes_fim, limit, offset)
    except Exception as e:
        logger.exception("Erro ao listar afastamentos por período")
        raise HTTPException(status_code=500, detail="Erro ao listar afastamentos por período")


@router.get("/duracao-minima", response_model=List[AfastamentoRead], summary="Listar afastamentos por duração mínima")
def listar_afastamentos_duracao_minima(
    duracao_minima: int = Query(..., gt=0),
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    session: Session = Depends(get_session)
):
    try:
        return buscar_por_duracao_minima(session, duracao_minima, limit, offset)
    except Exception as e:
        logger.exception("Erro ao listar afastamentos por duração mínima")
        raise HTTPException(status_code=500, detail="Erro ao listar afastamentos por duração")


@router.get("/longos", response_model=List[AfastamentoRead], summary="Listar afastamentos longos")
def listar_afastamentos_longos(
    limite_dias: int = Query(30, gt=0),
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    session: Session = Depends(get_session)
):
    try:
        return buscar_afastamentos_longos(session, limite_dias, limit, offset)
    except Exception as e:
        logger.exception("Erro ao listar afastamentos longos")
        raise HTTPException(status_code=500, detail="Erro ao listar afastamentos longos")


# COUNT - Contar afastamentos filtrados
@router.get("/contar/mes-ano", summary="Contar afastamentos por mês e ano")
def contar_afastamentos_mes_ano(
    ano: int = Query(..., ge=2000, le=2100),
    mes: int = Query(..., ge=1, le=12),
    session: Session = Depends(get_session)
):
    """Conta afastamentos por mês e ano específicos"""
    try:
        total = contar_afastamentos_filtrados(session=session, ano=ano, mes=mes)
        return {"ano": ano, "mes": mes, "quantidade": total}
    except Exception as e:
        logger.exception("Erro ao contar afastamentos")
        raise HTTPException(status_code=500, detail="Erro ao contar afastamentos")


# COUNT - Contar total
@router.get("/contar/total", summary="Contar total de afastamentos")
def contar_total_afastamentos_endpoint(
    session: Session = Depends(get_session)
):
    """Conta o total de afastamentos na base"""
    try:
        total = contar_total_afastamentos(session)
        return {"quantidade_total": total}
    except Exception as e:
        logger.exception("Erro ao contar total de afastamentos")
        raise HTTPException(status_code=500, detail="Erro ao contar total de afastamentos")


# COUNT - Contar por servidor
@router.get("/contar/servidor/{id_servidor}", summary="Contar afastamentos por servidor")
def contar_afastamentos_servidor(
    id_servidor: int = Path(..., gt=0),
    ano: Optional[int] = Query(None, ge=2000, le=2100),
    mes: Optional[int] = Query(None, ge=1, le=12),
    session: Session = Depends(get_session)
):
    """Conta afastamentos de um servidor específico"""
    try:
        total = contar_afastamentos_por_servidor(session, id_servidor, ano, mes)
        return {"id_servidor": id_servidor, "ano": ano, "mes": mes, "quantidade": total}
    except Exception as e:
        logger.exception("Erro ao contar afastamentos por servidor")
        raise HTTPException(status_code=500, detail="Erro ao contar afastamentos do servidor")


# DELETE - Deletar por servidor
@router.delete("/servidor/{id_servidor}", summary="Deletar afastamentos por servidor")
def deletar_afastamentos_servidor(
    id_servidor: int = Path(..., gt=0),
    ano: Optional[int] = Query(None, ge=2000, le=2100),
    mes: Optional[int] = Query(None, ge=1, le=12),
    session: Session = Depends(get_session)
):
    """Deleta afastamentos de um servidor específico"""
    try:
        quantidade_deletada = deletar_afastamentos_por_servidor(session, id_servidor, ano, mes)
        return {
            "mensagem": f"{quantidade_deletada} afastamentos deletados",
            "id_servidor": id_servidor,
            "ano": ano,
            "mes": mes,
            "quantidade_deletada": quantidade_deletada
        }
    except Exception as e:
        logger.exception("Erro ao deletar afastamentos por servidor")
        raise HTTPException(status_code=500, detail="Erro ao deletar afastamentos do servidor")


# STATISTICS - Estatísticas do servidor
@router.get("/estatisticas/servidor/{id_servidor}", response_model=Dict[str, Any], summary="Obter estatísticas de afastamentos por servidor")
def obter_estatisticas_servidor_endpoint(
    id_servidor: int = Path(..., gt=0),
    ano: Optional[int] = Query(None, ge=2000, le=2100),
    session: Session = Depends(get_session)
):
    """Obtém estatísticas completas de afastamentos de um servidor"""
    try:
        estatisticas = obter_estatisticas_servidor(session, id_servidor, ano)
        return {
            "id_servidor": id_servidor,
            "ano": ano,
            **estatisticas
        }
    except Exception as e:
        logger.exception("Erro ao obter estatísticas do servidor")
        raise HTTPException(status_code=500, detail="Erro ao obter estatísticas do servidor")



# READ - Buscar por ID
@router.get("/{id_afastamento}", response_model=AfastamentoRead, summary="Buscar afastamento por ID")
def buscar_afastamento_por_id(
    id_afastamento: int = Path(..., gt=0),
    session: Session = Depends(get_session)
):
    """Busca um afastamento específico pelo ID"""
    try:
        afastamento = buscar_por_id(session, id_afastamento)
        if not afastamento:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Afastamento não encontrado"
            )
        return afastamento
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Erro ao buscar afastamento por ID")
        raise HTTPException(status_code=500, detail="Erro ao buscar afastamento")


# UPDATE - Atualização parcial
@router.patch("/{id_afastamento}", response_model=AfastamentoRead, summary="Atualizar afastamento parcialmente")
def atualizar_afastamento_endpoint(
    id_afastamento: int = Path(..., gt=0),
    dados_atualizacao: AfastamentoUpdate = None,
    session: Session = Depends(get_session)
):
    """Atualiza parcialmente um afastamento"""
    try:
        if not dados_atualizacao:
            raise HTTPException(status_code=400, detail="Dados para atualização são obrigatórios")
        
        # Remove campos None do dicionário
        dados_dict = {k: v for k, v in dados_atualizacao.dict(exclude_unset=True).items() if v is not None}
        
        afastamento_atualizado = atualizar_afastamento(session, id_afastamento, dados_dict)
        if not afastamento_atualizado:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Afastamento não encontrado"
            )
        return afastamento_atualizado
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Erro ao atualizar afastamento")
        raise HTTPException(status_code=500, detail="Erro ao atualizar afastamento")


# UPDATE - Atualização completa
@router.put("/{id_afastamento}", response_model=AfastamentoRead, summary="Atualizar afastamento completamente")
def atualizar_afastamento_completo_endpoint(
    id_afastamento: int = Path(..., gt=0),
    afastamento_data: AfastamentoCreate = None,
    session: Session = Depends(get_session)
):
    """Atualiza completamente um afastamento"""
    try:
        if not afastamento_data:
            raise HTTPException(status_code=400, detail="Dados do afastamento são obrigatórios")
        
        from app.models.afastamento import Afastamento
        afastamento_novo = Afastamento(**afastamento_data.dict())
        
        afastamento_atualizado = atualizar_afastamento_completo(session, id_afastamento, afastamento_novo)
        if not afastamento_atualizado:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Afastamento não encontrado"
            )
        return afastamento_atualizado
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Erro ao atualizar afastamento completamente")
        raise HTTPException(status_code=500, detail="Erro ao atualizar afastamento")


# DELETE - Deletar afastamento
@router.delete("/{id_afastamento}", status_code=status.HTTP_204_NO_CONTENT, summary="Deletar afastamento")
def deletar_afastamento_endpoint(
    id_afastamento: int = Path(..., gt=0),
    session: Session = Depends(get_session)
):
    """Deleta um afastamento específico"""
    try:
        sucesso = deletar_afastamento(session, id_afastamento)
        if not sucesso:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Afastamento não encontrado"
            )
        return None
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Erro ao deletar afastamento")
        raise HTTPException(status_code=500, detail="Erro ao deletar afastamento")


# READ - Listar todos os afastamentos
@router.get("/", response_model=List[AfastamentoRead], summary="Listar todos os afastamentos")
def listar_todos_afastamentos(
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    session: Session = Depends(get_session)
):
    """Lista todos os afastamentos com paginação"""
    try:
        return listar_todos(session, limit=limit, offset=offset)
    except Exception as e:
        logger.exception("Erro ao listar todos os afastamentos")
        raise HTTPException(status_code=500, detail="Erro ao listar afastamentos")




