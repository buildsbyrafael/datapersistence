from fastapi import APIRouter, Depends, Query, HTTPException, status, UploadFile, File, Path
from sqlmodel import Session
from typing import List, Optional
import logging
import pandas as pd
from io import StringIO

from app.core.database import get_session
from app.models.servidor import Servidor
from app.schemas.servidor import ServidorRead, ServidorCreate
from app.crud.servidor import (
    buscar_com_filtros, 
    contar_com_filtros,
    # Novas importações para as operações CRUD completas
    criar_servidor,
    buscar_por_id,
    listar_todos,
    atualizar_servidor,
    atualizar_servidor_completo,
    deletar_servidor,
    deletar_servidores_em_lote,
    verificar_cpf_existe,
    buscar_por_cpf
)
from app.utils.importar_servidores import importar_servidores_dataframe

router = APIRouter(prefix="/servidores", tags=["Servidores"])
logger = logging.getLogger(__name__)


@router.put("/importar", summary="Importar CSV de servidores")
async def importar_servidores_csv_api(
    arquivo: UploadFile = File(...)
):
    if not arquivo.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="O arquivo deve ser um CSV.")

    try:
        conteudo = await arquivo.read()
        df = pd.read_csv(StringIO(conteudo.decode("latin1")), dtype=str, sep=";")

        with next(get_session()) as session:
            total = importar_servidores_dataframe(df, session)

        return {"mensagem": f"{total} servidores importados com sucesso!"}

    except Exception as e:
        logger.exception("Erro ao importar servidores via API")
        raise HTTPException(status_code=500, detail=f"Erro interno: {e}")

@router.get("/", response_model=List[ServidorRead])
def listar_servidores(
    nome: Optional[str] = Query(None),
    org_exercicio: Optional[str] = Query(None),
    cpf_parcial: Optional[str] = Query(None),
    descr_cargo: Optional[str] = Query(None),
    org_superior: Optional[str] = Query(None),
    regime: Optional[str] = Query(None),
    jornada_trabalho: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    session: Session = Depends(get_session)
):
    try:
        resultados = buscar_com_filtros(
            session=session,
            nome=nome,
            org_exercicio=org_exercicio,
            cpf_parcial=cpf_parcial,
            descr_cargo=descr_cargo,
            org_superior=org_superior,
            regime=regime,
            jornada_trabalho=jornada_trabalho,
            limit=limit,
            offset=offset
        )

        logger.info(f"[DEBUG] Total de registros retornados pela query: {len(resultados)}")

        validados = []
        for servidor in resultados:
            try:
                validado = ServidorRead.model_validate(servidor)
                validados.append(validado)
                logger.info(f"[DEBUG] Validado: {validado.id_servidor} - {validado.nome}")
            except Exception as e:
                logger.error(f"[ERRO] Falha ao validar servidor {getattr(servidor, 'id_servidor', '?')}: {e}")

        return validados

    except Exception as e:
        logger.exception("Erro ao buscar servidores!")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Erro ao buscar servidores!"
        )





@router.get("/contar", summary="Contar servidores com base nos filtros")
def contar_servidores(
    nome: Optional[str] = Query(None),
    org_exercicio: Optional[str] = Query(None),
    cpf_parcial: Optional[str] = Query(None),
    descr_cargo: Optional[str] = Query(None),
    org_superior: Optional[str] = Query(None),
    regime: Optional[str] = Query(None),
    jornada_trabalho: Optional[str] = Query(None),
    session: Session = Depends(get_session)
):
    try:
        total = contar_com_filtros(
            session=session,
            nome=nome,
            org_exercicio=org_exercicio,
            cpf_parcial=cpf_parcial,
            descr_cargo=descr_cargo,
            org_superior=org_superior,
            regime=regime,
            jornada_trabalho=jornada_trabalho
        )
        return {"quantidade": total}
    except Exception as e:
        logger.exception("Erro ao contar servidores!")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Erro ao contar servidores!"
        )


# ========== NOVAS ROTAS CRUD ==========

@router.post("/", response_model=ServidorRead, status_code=status.HTTP_201_CREATED)
def criar_novo_servidor(
    servidor_data: ServidorCreate,
    session: Session = Depends(get_session)
):
    """Criar um novo servidor"""
    try:
        # Verificar se CPF já existe
        if verificar_cpf_existe(session, servidor_data.cpf):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="CPF já existe no sistema"
            )
        
        # Criar novo servidor
        novo_servidor = Servidor(**servidor_data.model_dump())
        servidor_criado = criar_servidor(session, novo_servidor)
        
        return ServidorRead.model_validate(servidor_criado)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Erro ao criar servidor")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Erro ao criar servidor"
        )


@router.get("/todos", response_model=List[ServidorRead])
def listar_todos_servidores(
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    session: Session = Depends(get_session)
):
    """Listar todos os servidores sem filtros"""
    try:
        servidores = listar_todos(session, limit=limit, offset=offset)
        return [ServidorRead.model_validate(servidor) for servidor in servidores]
    except Exception as e:
        logger.exception("Erro ao listar todos os servidores")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Erro ao listar servidores"
        )


@router.get("/{id_servidor}", response_model=ServidorRead)
def buscar_servidor_por_id(
    id_servidor: int = Path(..., description="ID do servidor"),
    session: Session = Depends(get_session)
):
    """Buscar servidor por ID"""
    servidor = buscar_por_id(session, id_servidor)
    if not servidor:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Servidor não encontrado"
        )
    return ServidorRead.model_validate(servidor)


@router.get("/cpf/{cpf}", response_model=ServidorRead)
def buscar_servidor_por_cpf(
    cpf: str = Path(..., description="CPF do servidor"),
    session: Session = Depends(get_session)
):
    """Buscar servidor por CPF"""
    servidor = buscar_por_cpf(session, cpf)
    if not servidor:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Servidor não encontrado"
        )
    return ServidorRead.model_validate(servidor)


@router.put("/{id_servidor}", response_model=ServidorRead)
def atualizar_servidor_por_id(
    id_servidor: int = Path(..., description="ID do servidor"),
    servidor_data: ServidorCreate = None,
    nome: Optional[str] = Query(None),
    cpf: Optional[str] = Query(None),
    descr_cargo: Optional[str] = Query(None),
    org_superior: Optional[str] = Query(None),
    org_exercicio: Optional[str] = Query(None),
    regime: Optional[str] = Query(None),
    jornada_trabalho: Optional[str] = Query(None),
    session: Session = Depends(get_session)
):
    """Atualizar servidor por ID"""
    try:
        # Verificar se servidor existe
        servidor_existente = buscar_por_id(session, id_servidor)
        if not servidor_existente:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Servidor não encontrado"
            )
        
        # Verificar CPF duplicado (se está sendo alterado)
        novo_cpf = cpf if cpf else (servidor_data.cpf if servidor_data else None)
        if novo_cpf and verificar_cpf_existe(session, novo_cpf, excluir_id=id_servidor):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="CPF já existe no sistema"
            )
        
        # Atualizar usando dados do body ou query params
        if servidor_data:
            dados_atualizacao = servidor_data.model_dump(exclude_unset=True)
            servidor_atualizado = atualizar_servidor(session, id_servidor, dados_atualizacao)
        else:
            servidor_atualizado = atualizar_servidor_completo(
                session=session,
                id_servidor=id_servidor,
                nome=nome,
                cpf=cpf,
                descr_cargo=descr_cargo,
                org_superior=org_superior,
                org_exercicio=org_exercicio,
                regime=regime,
                jornada_trabalho=jornada_trabalho
            )
        
        return ServidorRead.model_validate(servidor_atualizado)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Erro ao atualizar servidor {id_servidor}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Erro ao atualizar servidor"
        )


@router.patch("/{id_servidor}", response_model=ServidorRead)
def atualizar_parcial_servidor(
    id_servidor: int = Path(..., description="ID do servidor"),
    dados_atualizacao: dict = None,
    session: Session = Depends(get_session)
):
    """Atualização parcial de servidor usando PATCH"""
    try:
        # Verificar se servidor existe
        servidor_existente = buscar_por_id(session, id_servidor)
        if not servidor_existente:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Servidor não encontrado"
            )
        
        # Verificar CPF duplicado (se está sendo alterado)
        if dados_atualizacao and "cpf" in dados_atualizacao:
            if verificar_cpf_existe(session, dados_atualizacao["cpf"], excluir_id=id_servidor):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="CPF já existe no sistema"
                )
        
        servidor_atualizado = atualizar_servidor(session, id_servidor, dados_atualizacao or {})
        if not servidor_atualizado:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Servidor não encontrado"
            )
        
        return ServidorRead.model_validate(servidor_atualizado)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Erro ao atualizar parcialmente servidor {id_servidor}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Erro ao atualizar servidor"
        )


@router.delete("/{id_servidor}")
def deletar_servidor_por_id(
    id_servidor: int = Path(..., description="ID do servidor"),
    session: Session = Depends(get_session)
):
    """Deletar servidor por ID"""
    try:
        sucesso = deletar_servidor(session, id_servidor)
        if not sucesso:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Servidor não encontrado"
            )
        
        return {"mensagem": f"Servidor {id_servidor} deletado com sucesso"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Erro ao deletar servidor {id_servidor}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Erro ao deletar servidor"
        )


@router.delete("/lote/deletar")
def deletar_servidores_lote(
    ids_servidores: List[int],
    session: Session = Depends(get_session)
):
    """Deletar múltiplos servidores por lista de IDs"""
    try:
        if not ids_servidores:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Lista de IDs não pode estar vazia"
            )
        
        total_deletados = deletar_servidores_em_lote(session, ids_servidores)
        
        return {
            "mensagem": f"{total_deletados} servidores deletados com sucesso",
            "total_deletados": total_deletados,
            "ids_solicitados": len(ids_servidores)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Erro ao deletar servidores em lote")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Erro ao deletar servidores"
        )


@router.get("/validar/cpf/{cpf}")
def validar_cpf_disponivel(
    cpf: str = Path(..., description="CPF a ser validado"),
    excluir_id: Optional[int] = Query(None, description="ID do servidor a excluir da validação"),
    session: Session = Depends(get_session)
):
    """Verificar se CPF está disponível"""
    try:
        cpf_existe = verificar_cpf_existe(session, cpf, excluir_id)
        return {
            "cpf": cpf,
            "disponivel": not cpf_existe,
            "mensagem": "CPF já existe" if cpf_existe else "CPF disponível"
        }
    except Exception as e:
        logger.exception(f"Erro ao validar CPF {cpf}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Erro ao validar CPF"
        )