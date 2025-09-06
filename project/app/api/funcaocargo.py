from fastapi import APIRouter, UploadFile, File, HTTPException, status, Depends, Query
from sqlmodel import Session
from typing import List, Optional
from io import StringIO
import pandas as pd
import logging

from app.core.database import get_session
from app.schemas.funcaocargo import FuncaoCargoRead, FuncaoCargoCreate, FuncaoCargoUpdate
from app.utils.importar_funcaocargo import importar_funcaocargo_dataframe
from app.crud.funcaocargo import (
    listar_por_servidor, listar_geral, contar_funcoescargos_filtradas,
    criar_funcaocargo, buscar_por_id, atualizar_funcaocargo, deletar_funcaocargo,
    listar_por_cargo_funcao, buscar_por_servidor_e_cargo, verificar_existe,
    listar_com_relacionamentos, deletar_por_servidor
)
from app.models.funcaocargo import FuncaoCargo

router = APIRouter(prefix="/funcoescargos", tags=["Funções e Cargos"])
logger = logging.getLogger(__name__)


@router.put("/importar", summary="Importar CSV de funções e cargos por servidor")
async def importar_funcoescargos_csv_api(
    arquivo: UploadFile = File(...)
):
    if not arquivo.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="O arquivo deve ser um CSV.")

    try:
        conteudo = await arquivo.read()
        df = pd.read_csv(StringIO(conteudo.decode("latin1")), dtype=str, sep=";")

        with next(get_session()) as session:
            total = importar_funcaocargo_dataframe(df, session)

        return {"mensagem": f"{total} vínculos de função/cargo importados com sucesso!"}

    except Exception as e:
        logger.exception("Erro ao importar funções/cargos por servidor via API")
        raise HTTPException(status_code=500, detail=f"Erro interno: {e}")


@router.get("/", response_model=List[FuncaoCargoRead], summary="Listar funções/cargos por vínculo")
def listar_funcoescargos(
    id_servidor: Optional[int] = Query(None),
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    session: Session = Depends(get_session)
):
    try:
        if id_servidor is not None:
            return listar_por_servidor(session, id_servidor=id_servidor, limit=limit, offset=offset)
        return listar_geral(session, limit=limit, offset=offset)

    except Exception as e:
        logger.exception("Erro ao listar vínculos de funções/cargos")
        raise HTTPException(status_code=500, detail="Erro ao listar vínculos de funções/cargos")


@router.get("/contar", summary="Contar vínculos de funções/cargos com base no filtro de id_servidor")
def contar_funcoescargos(
    id_servidor: Optional[int] = Query(None),
    session: Session = Depends(get_session)
):
    try:
        total = contar_funcoescargos_filtradas(session=session, id_servidor=id_servidor)
        return {"quantidade": total}
    except Exception as e:
        logger.exception("Erro ao contar vínculos de funções/cargos")
        raise HTTPException(status_code=500, detail="Erro ao contar vínculos de funções/cargos")


# NOVAS ROTAS ADICIONADAS

@router.post("/", response_model=FuncaoCargoRead, status_code=status.HTTP_201_CREATED, summary="Criar nova função/cargo")
def criar_nova_funcaocargo(
    funcaocargo_data: FuncaoCargoCreate,
    session: Session = Depends(get_session)
):
    """Cria uma nova associação entre servidor e função/cargo."""
    try:
        # Verifica se já existe a associação
        if verificar_existe(session, funcaocargo_data.id_servidor, funcaocargo_data.id_cargo_funcao):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Já existe uma associação entre este servidor e função/cargo"
            )
        
        funcaocargo = FuncaoCargo(**funcaocargo_data.model_dump())
        return criar_funcaocargo(session, funcaocargo)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Erro ao criar função/cargo")
        raise HTTPException(status_code=500, detail="Erro ao criar função/cargo")


@router.get("/{id_servidor_funcao}", response_model=FuncaoCargoRead, summary="Buscar função/cargo por ID")
def buscar_funcaocargo_por_id(
    id_servidor_funcao: int,
    session: Session = Depends(get_session)
):
    """Busca uma função/cargo específica pelo ID."""
    try:
        funcaocargo = buscar_por_id(session, id_servidor_funcao)
        if not funcaocargo:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Função/cargo não encontrada"
            )
        return funcaocargo
        
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Erro ao buscar função/cargo por ID")
        raise HTTPException(status_code=500, detail="Erro ao buscar função/cargo")


@router.put("/{id_servidor_funcao}", response_model=FuncaoCargoRead, summary="Atualizar função/cargo")
def atualizar_funcaocargo_endpoint(
    id_servidor_funcao: int,
    dados_atualizacao: FuncaoCargoUpdate,
    session: Session = Depends(get_session)
):
    """Atualiza uma função/cargo existente."""
    try:
        # Remove campos None do dicionário de atualização
        dados_dict = {k: v for k, v in dados_atualizacao.model_dump().items() if v is not None}
        
        funcaocargo_atualizada = atualizar_funcaocargo(session, id_servidor_funcao, dados_dict)
        if not funcaocargo_atualizada:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Função/cargo não encontrada"
            )
        return funcaocargo_atualizada
        
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Erro ao atualizar função/cargo")
        raise HTTPException(status_code=500, detail="Erro ao atualizar função/cargo")


@router.delete("/{id_servidor_funcao}", summary="Deletar função/cargo")
def deletar_funcaocargo_endpoint(
    id_servidor_funcao: int,
    session: Session = Depends(get_session)
):
    """Deleta uma função/cargo específica."""
    try:
        sucesso = deletar_funcaocargo(session, id_servidor_funcao)
        if not sucesso:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Função/cargo não encontrada"
            )
        return {"mensagem": "Função/cargo deletada com sucesso"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Erro ao deletar função/cargo")
        raise HTTPException(status_code=500, detail="Erro ao deletar função/cargo")


@router.get("/cargo/{id_cargo_funcao}", response_model=List[FuncaoCargoRead], summary="Listar por cargo/função")
def listar_por_cargo_funcao_endpoint(
    id_cargo_funcao: int,
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    session: Session = Depends(get_session)
):
    """Lista todas as associações de um cargo/função específico."""
    try:
        return listar_por_cargo_funcao(session, id_cargo_funcao, limit, offset)
        
    except Exception as e:
        logger.exception("Erro ao listar por cargo/função")
        raise HTTPException(status_code=500, detail="Erro ao listar por cargo/função")


@router.get("/servidor/{id_servidor}/cargo/{id_cargo_funcao}", response_model=FuncaoCargoRead, summary="Buscar por servidor e cargo")
def buscar_por_servidor_e_cargo_endpoint(
    id_servidor: int,
    id_cargo_funcao: int,
    session: Session = Depends(get_session)
):
    """Busca uma associação específica entre servidor e cargo/função."""
    try:
        funcaocargo = buscar_por_servidor_e_cargo(session, id_servidor, id_cargo_funcao)
        if not funcaocargo:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Associação não encontrada"
            )
        return funcaocargo
        
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Erro ao buscar por servidor e cargo")
        raise HTTPException(status_code=500, detail="Erro ao buscar por servidor e cargo")


@router.get("/servidor/{id_servidor}/cargo/{id_cargo_funcao}/existe", summary="Verificar se associação existe")
def verificar_associacao_existe(
    id_servidor: int,
    id_cargo_funcao: int,
    session: Session = Depends(get_session)
):
    """Verifica se existe uma associação entre servidor e cargo/função."""
    try:
        existe = verificar_existe(session, id_servidor, id_cargo_funcao)
        return {"existe": existe}
        
    except Exception as e:
        logger.exception("Erro ao verificar existência da associação")
        raise HTTPException(status_code=500, detail="Erro ao verificar existência da associação")


@router.get("/com-relacionamentos/", response_model=List[FuncaoCargoRead], summary="Listar com relacionamentos")
def listar_com_relacionamentos_endpoint(
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    session: Session = Depends(get_session)
):
    """Lista funções/cargos com relacionamentos carregados."""
    try:
        return listar_com_relacionamentos(session, limit, offset)
        
    except Exception as e:
        logger.exception("Erro ao listar com relacionamentos")
        raise HTTPException(status_code=500, detail="Erro ao listar com relacionamentos")


@router.delete("/servidor/{id_servidor}/todas", summary="Deletar todas as funções/cargos de um servidor")
def deletar_todas_por_servidor(
    id_servidor: int,
    session: Session = Depends(get_session)
):
    """Deleta todas as associações de função/cargo de um servidor específico."""
    try:
        count = deletar_por_servidor(session, id_servidor)
        return {"mensagem": f"{count} associações deletadas com sucesso"}
        
    except Exception as e:
        logger.exception("Erro ao deletar associações por servidor")
        raise HTTPException(status_code=500, detail="Erro ao deletar associações por servidor")


@router.get("/contar/filtros", summary="Contar com múltiplos filtros")
def contar_com_filtros(
    id_servidor: Optional[int] = Query(None),
    id_cargo_funcao: Optional[int] = Query(None),
    session: Session = Depends(get_session)
):
    """Conta vínculos com filtros opcionais por servidor e/ou cargo/função."""
    try:
        total = contar_funcoescargos_filtradas(
            session=session, 
            id_servidor=id_servidor, 
            id_cargo_funcao=id_cargo_funcao
        )
        return {"quantidade": total, "filtros": {"id_servidor": id_servidor, "id_cargo_funcao": id_cargo_funcao}}
        
    except Exception as e:
        logger.exception("Erro ao contar com filtros")
        raise HTTPException(status_code=500, detail="Erro ao contar com filtros")