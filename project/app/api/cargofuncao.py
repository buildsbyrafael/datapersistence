from fastapi import APIRouter, UploadFile, File, HTTPException, status, Depends, Query, Path
from sqlmodel import Session
from typing import List, Optional, Dict, Any
from io import StringIO
import pandas as pd
import logging

from app.core.database import get_session
from app.schemas.cargofuncao import CargoFuncaoRead, CargoFuncaoCreate
from app.utils.importar_cargosfuncoes import importar_cargosfuncoes_dataframe
from app.crud.cargofuncao import (
    listar_cargosfuncoes, 
    contar_cargosfuncoes_filtradas,
    criar_cargo_funcao,
    buscar_por_id,
    atualizar_cargo_funcao,
    atualizar_cargo_funcao_completo,
    deletar_cargo_funcao,
    deletar_multiplos_cargosfuncoes,
    verificar_existencia,
    buscar_por_descricao_like,
    contar_total_cargosfuncoes,
    listar_classes_cargo_distintas,
    listar_funcoes_distintas,
    buscar_por_multiplos_ids,
    listar_por_nivel_cargo_range,
    buscar_duplicado
)
from app.models.cargofuncao import CargoFuncao

router = APIRouter(prefix="/cargosfuncoes", tags=["Cargos e Funções"])
logger = logging.getLogger(__name__)


@router.put("/importar", summary="Importar CSV de cargos e funções")
async def importar_cargosfuncoes_csv_api(
    arquivo: UploadFile = File(...)
):
    if not arquivo.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="O arquivo deve ser um CSV.")

    try:
        conteudo = await arquivo.read()
        df = pd.read_csv(StringIO(conteudo.decode("latin1")), dtype=str, sep=";")

        with next(get_session()) as session:
            total = importar_cargosfuncoes_dataframe(df, session)

        return {"mensagem": f"{total} cargos/funções importados com sucesso!"}

    except Exception as e:
        logger.exception("Erro ao importar cargos/funções via API")
        raise HTTPException(status_code=500, detail=f"Erro interno: {e}")


@router.post("/", response_model=CargoFuncaoRead, status_code=status.HTTP_201_CREATED, summary="Criar novo cargo/função")
def criar_cargo_funcao_api(
    cargo_funcao_data: CargoFuncaoCreate,
    session: Session = Depends(get_session)
):
    try:
        # Verificar se já existe um registro duplicado
        duplicado = buscar_duplicado(
            session=session,
            classe_cargo=cargo_funcao_data.classe_cargo,
            referencia_cargo=cargo_funcao_data.referencia_cargo,
            padrao_cargo=cargo_funcao_data.padrao_cargo,
            nivel_cargo=cargo_funcao_data.nivel_cargo,
            funcao=cargo_funcao_data.funcao,
            descricao_cargo=cargo_funcao_data.descricao_cargo,
            nivel_funcao=cargo_funcao_data.nivel_funcao
        )
        
        if duplicado:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Já existe um cargo/função com essas características"
            )
        
        # Criar o novo cargo/função
        cargo_funcao = CargoFuncao(**cargo_funcao_data.model_dump())
        return criar_cargo_funcao(session, cargo_funcao)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Erro ao criar cargo/função")
        raise HTTPException(status_code=500, detail="Erro ao criar cargo/função")


@router.get("/", response_model=List[CargoFuncaoRead], summary="Listar cargos/funções com filtros e paginação")
def listar_cargosfuncoes_api(
    classe_cargo: Optional[str] = Query(None),
    referencia_cargo: Optional[int] = Query(None),
    padrao_cargo: Optional[int] = Query(None),
    nivel_cargo: Optional[int] = Query(None),
    funcao: Optional[str] = Query(None),
    descricao_cargo: Optional[str] = Query(None),
    nivel_funcao: Optional[int] = Query(None),
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    session: Session = Depends(get_session)
):
    try:
        return listar_cargosfuncoes(
            session=session,
            classe_cargo=classe_cargo,
            referencia_cargo=referencia_cargo,
            padrao_cargo=padrao_cargo,
            nivel_cargo=nivel_cargo,
            funcao=funcao,
            descricao_cargo=descricao_cargo,
            nivel_funcao=nivel_funcao,
            limit=limit,
            offset=offset
        )
    except Exception as e:
        logger.exception("Erro ao listar cargos/funções")
        raise HTTPException(status_code=500, detail="Erro ao listar cargos/funções")


@router.get("/contar", summary="Contar cargos/funções com base em filtros")
def contar_cargosfuncoes_endpoint(
    classe_cargo: Optional[str] = Query(None),
    referencia_cargo: Optional[int] = Query(None),
    padrao_cargo: Optional[int] = Query(None),
    nivel_cargo: Optional[int] = Query(None),
    funcao: Optional[str] = Query(None),
    descricao_cargo: Optional[str] = Query(None),
    nivel_funcao: Optional[int] = Query(None),
    session: Session = Depends(get_session)
):
    try:
        total = contar_cargosfuncoes_filtradas(
            session=session,
            classe_cargo=classe_cargo,
            referencia_cargo=referencia_cargo,
            padrao_cargo=padrao_cargo,
            nivel_cargo=nivel_cargo,
            funcao=funcao,
            descricao_cargo=descricao_cargo,
            nivel_funcao=nivel_funcao
        )
        return {"quantidade": total}
    except Exception as e:
        logger.exception("Erro ao contar cargos/funções")
        raise HTTPException(status_code=500, detail="Erro ao contar cargos/funções")


@router.get("/total", summary="Contar total de cargos/funções")
def contar_total_cargosfuncoes_endpoint(session: Session = Depends(get_session)):
    try:
        total = contar_total_cargosfuncoes(session)
        return {"total": total}
    except Exception as e:
        logger.exception("Erro ao contar total de cargos/funções")
        raise HTTPException(status_code=500, detail="Erro ao contar total de cargos/funções")


@router.get("/classes-distintas", response_model=List[str], summary="Listar classes de cargo distintas")
def listar_classes_cargo_distintas_endpoint(session: Session = Depends(get_session)):
    try:
        return listar_classes_cargo_distintas(session)
    except Exception as e:
        logger.exception("Erro ao listar classes de cargo distintas")
        raise HTTPException(status_code=500, detail="Erro ao listar classes de cargo")


@router.get("/funcoes-distintas", response_model=List[str], summary="Listar funções distintas")
def listar_funcoes_distintas_endpoint(session: Session = Depends(get_session)):
    try:
        return listar_funcoes_distintas(session)
    except Exception as e:
        logger.exception("Erro ao listar funções distintas")
        raise HTTPException(status_code=500, detail="Erro ao listar funções")


@router.get("/buscar-descricao", response_model=List[CargoFuncaoRead], summary="Buscar por descrição parcial")
def buscar_por_descricao_like_endpoint(
    descricao: str = Query(..., min_length=2, description="Texto para busca na descrição"),
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    session: Session = Depends(get_session)
):
    try:
        return buscar_por_descricao_like(session, descricao, limit, offset)
    except Exception as e:
        logger.exception("Erro ao buscar por descrição")
        raise HTTPException(status_code=500, detail="Erro ao buscar por descrição")


@router.get("/nivel-range", response_model=List[CargoFuncaoRead], summary="Listar por intervalo de níveis de cargo")
def listar_por_nivel_cargo_range_endpoint(
    nivel_min: int = Query(..., ge=1, description="Nível mínimo"),
    nivel_max: int = Query(..., ge=1, description="Nível máximo"),
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    session: Session = Depends(get_session)
):
    if nivel_min > nivel_max:
        raise HTTPException(status_code=400, detail="Nível mínimo deve ser menor ou igual ao máximo")
    
    try:
        return listar_por_nivel_cargo_range(session, nivel_min, nivel_max, limit, offset)
    except Exception as e:
        logger.exception("Erro ao listar por intervalo de níveis")
        raise HTTPException(status_code=500, detail="Erro ao listar por intervalo de níveis")


@router.get("/multiplos-ids", response_model=List[CargoFuncaoRead], summary="Buscar múltiplos cargos/funções por IDs")
def buscar_por_multiplos_ids_endpoint(
    ids: List[int] = Query(..., description="Lista de IDs separados por vírgula"),
    session: Session = Depends(get_session)
):
    if len(ids) > 100:
        raise HTTPException(status_code=400, detail="Máximo de 100 IDs por consulta")
    
    try:
        return buscar_por_multiplos_ids(session, ids)
    except Exception as e:
        logger.exception("Erro ao buscar múltiplos IDs")
        raise HTTPException(status_code=500, detail="Erro ao buscar múltiplos IDs")


@router.get("/{id_cargo_funcao}", response_model=CargoFuncaoRead, summary="Buscar cargo/função por ID")
def buscar_cargo_funcao_por_id(
    id_cargo_funcao: int = Path(..., gt=0, description="ID do cargo/função"),
    session: Session = Depends(get_session)
):
    try:
        cargo_funcao = buscar_por_id(session, id_cargo_funcao)
        if not cargo_funcao:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Cargo/função não encontrado"
            )
        return cargo_funcao
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Erro ao buscar cargo/função por ID")
        raise HTTPException(status_code=500, detail="Erro ao buscar cargo/função")


@router.put("/{id_cargo_funcao}", response_model=CargoFuncaoRead, summary="Atualizar cargo/função completo")
def atualizar_cargo_funcao_completo_endpoint(
    id_cargo_funcao: int = Path(..., gt=0, description="ID do cargo/função"),
    dados_atualizacao: CargoFuncaoCreate = ...,
    session: Session = Depends(get_session)
):
    try:
        cargo_funcao_atualizado = atualizar_cargo_funcao_completo(
            session=session,
            id_cargo_funcao=id_cargo_funcao,
            classe_cargo=dados_atualizacao.classe_cargo,
            referencia_cargo=dados_atualizacao.referencia_cargo,
            padrao_cargo=dados_atualizacao.padrao_cargo,
            nivel_cargo=dados_atualizacao.nivel_cargo,
            funcao=dados_atualizacao.funcao,
            descricao_cargo=dados_atualizacao.descricao_cargo,
            nivel_funcao=dados_atualizacao.nivel_funcao
        )
        
        if not cargo_funcao_atualizado:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Cargo/função não encontrado"
            )
        
        return cargo_funcao_atualizado
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Erro ao atualizar cargo/função")
        raise HTTPException(status_code=500, detail="Erro ao atualizar cargo/função")


@router.patch("/{id_cargo_funcao}", response_model=CargoFuncaoRead, summary="Atualizar cargo/função parcial")
def atualizar_cargo_funcao_parcial_endpoint(
    id_cargo_funcao: int = Path(..., gt=0, description="ID do cargo/função"),
    dados_atualizacao: Dict[str, Any] = ...,
    session: Session = Depends(get_session)
):
    try:
        # Verificar se o cargo/função existe
        if not verificar_existencia(session, id_cargo_funcao):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Cargo/função não encontrado"
            )
        
        cargo_funcao_atualizado = atualizar_cargo_funcao(
            session=session,
            id_cargo_funcao=id_cargo_funcao,
            dados_atualizacao=dados_atualizacao
        )
        
        return cargo_funcao_atualizado
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Erro ao atualizar cargo/função parcialmente")
        raise HTTPException(status_code=500, detail="Erro ao atualizar cargo/função")


@router.delete("/{id_cargo_funcao}", status_code=status.HTTP_204_NO_CONTENT, summary="Deletar cargo/função")
def deletar_cargo_funcao_endpoint(
    id_cargo_funcao: int = Path(..., gt=0, description="ID do cargo/função"),
    session: Session = Depends(get_session)
):
    try:
        deletado = deletar_cargo_funcao(session, id_cargo_funcao)
        if not deletado:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Cargo/função não encontrado"
            )
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Erro ao deletar cargo/função")
        raise HTTPException(status_code=500, detail="Erro ao deletar cargo/função")


@router.delete("/multiplos/deletar", summary="Deletar múltiplos cargos/funções")
def deletar_multiplos_cargosfuncoes_endpoint(
    ids: List[int] = Query(..., description="Lista de IDs a serem deletados"),
    session: Session = Depends(get_session)
):
    if len(ids) > 100:
        raise HTTPException(status_code=400, detail="Máximo de 100 IDs por operação")
    
    try:
        deletados = deletar_multiplos_cargosfuncoes(session, ids)
        return {"mensagem": f"{deletados} cargos/funções deletados com sucesso", "deletados": deletados}
    except Exception as e:
        logger.exception("Erro ao deletar múltiplos cargos/funções")
        raise HTTPException(status_code=500, detail="Erro ao deletar múltiplos cargos/funções")


@router.get("/{id_cargo_funcao}/existe", summary="Verificar se cargo/função existe")
def verificar_existencia_endpoint(
    id_cargo_funcao: int = Path(..., gt=0, description="ID do cargo/função"),
    session: Session = Depends(get_session)
):
    try:
        existe = verificar_existencia(session, id_cargo_funcao)
        return {"existe": existe, "id": id_cargo_funcao}
    except Exception as e:
        logger.exception("Erro ao verificar existência do cargo/função")
        raise HTTPException(status_code=500, detail="Erro ao verificar existência")