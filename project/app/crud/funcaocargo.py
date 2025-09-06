from typing import Optional, List
from sqlmodel import Session, select
from sqlalchemy import func
from app.models.funcaocargo import FuncaoCargo


def criar_funcaocargo(session: Session, funcaocargo: FuncaoCargo) -> FuncaoCargo:
    session.add(funcaocargo)
    session.commit()
    session.refresh(funcaocargo)
    return funcaocargo


def buscar_por_id(session: Session, id_servidor_funcao: int) -> Optional[FuncaoCargo]:
    return session.get(FuncaoCargo, id_servidor_funcao)


def atualizar_funcaocargo(
    session: Session, 
    id_servidor_funcao: int, 
    dados_atualizacao: dict
) -> Optional[FuncaoCargo]:
    """
    Atualiza uma função/cargo por ID.
    
    Args:
        session: Sessão do banco de dados
        id_servidor_funcao: ID da função/cargo a ser atualizada
        dados_atualizacao: Dicionário com os campos a serem atualizados
        
    Returns:
        FuncaoCargo atualizada ou None se não encontrada
    """
    funcaocargo = session.get(FuncaoCargo, id_servidor_funcao)
    if not funcaocargo:
        return None
    
    # Atualiza apenas os campos fornecidos
    for campo, valor in dados_atualizacao.items():
        if hasattr(funcaocargo, campo):
            setattr(funcaocargo, campo, valor)
    
    session.add(funcaocargo)
    session.commit()
    session.refresh(funcaocargo)
    return funcaocargo


def deletar_funcaocargo(session: Session, id_servidor_funcao: int) -> bool:
    """
    Deleta uma função/cargo por ID.
    
    Args:
        session: Sessão do banco de dados
        id_servidor_funcao: ID da função/cargo a ser deletada
        
    Returns:
        True se deletada com sucesso, False se não encontrada
    """
    funcaocargo = session.get(FuncaoCargo, id_servidor_funcao)
    if not funcaocargo:
        return False
    
    session.delete(funcaocargo)
    session.commit()
    return True


def listar_por_servidor(
    session: Session,
    id_servidor: int,
    limit: int = 50,
    offset: int = 0
) -> List[FuncaoCargo]:
    query = (
        select(FuncaoCargo)
        .where(FuncaoCargo.id_servidor == id_servidor)
        .offset(offset)
        .limit(limit)
    )
    return session.exec(query).all()


def listar_por_cargo_funcao(
    session: Session,
    id_cargo_funcao: int,
    limit: int = 50,
    offset: int = 0
) -> List[FuncaoCargo]:
    """
    Lista todas as funções/cargos por ID do cargo/função.
    
    Args:
        session: Sessão do banco de dados
        id_cargo_funcao: ID do cargo/função
        limit: Limite de resultados
        offset: Offset para paginação
        
    Returns:
        Lista de FuncaoCargo
    """
    query = (
        select(FuncaoCargo)
        .where(FuncaoCargo.id_cargo_funcao == id_cargo_funcao)
        .offset(offset)
        .limit(limit)
    )
    return session.exec(query).all()


def listar_geral(
    session: Session,
    limit: int = 50,
    offset: int = 0
) -> List[FuncaoCargo]:
    query = select(FuncaoCargo).offset(offset).limit(limit)
    return session.exec(query).all()


def buscar_por_servidor_e_cargo(
    session: Session,
    id_servidor: int,
    id_cargo_funcao: int
) -> Optional[FuncaoCargo]:
    """
    Busca uma função/cargo específica por servidor e cargo.
    
    Args:
        session: Sessão do banco de dados
        id_servidor: ID do servidor
        id_cargo_funcao: ID do cargo/função
        
    Returns:
        FuncaoCargo se encontrada, None caso contrário
    """
    query = select(FuncaoCargo).where(
        FuncaoCargo.id_servidor == id_servidor,
        FuncaoCargo.id_cargo_funcao == id_cargo_funcao
    )
    return session.exec(query).first()


def contar_funcoescargos_filtradas(
    session: Session,
    id_servidor: Optional[int] = None,
    id_cargo_funcao: Optional[int] = None
) -> int:
    """
    Conta o número total de funções/cargos com filtros opcionais.
    
    Args:
        session: Sessão do banco de dados
        id_servidor: ID do servidor (opcional)
        id_cargo_funcao: ID do cargo/função (opcional)
        
    Returns:
        Número total de registros
    """
    query = select(func.count()).select_from(FuncaoCargo)
    
    if id_servidor is not None:
        query = query.where(FuncaoCargo.id_servidor == id_servidor)
    
    if id_cargo_funcao is not None:
        query = query.where(FuncaoCargo.id_cargo_funcao == id_cargo_funcao)
    
    return session.exec(query).one()


def verificar_existe(
    session: Session,
    id_servidor: int,
    id_cargo_funcao: int
) -> bool:
    """
    Verifica se já existe uma função/cargo para o servidor e cargo especificados.
    
    Args:
        session: Sessão do banco de dados
        id_servidor: ID do servidor
        id_cargo_funcao: ID do cargo/função
        
    Returns:
        True se existe, False caso contrário
    """
    query = select(func.count()).select_from(FuncaoCargo).where(
        FuncaoCargo.id_servidor == id_servidor,
        FuncaoCargo.id_cargo_funcao == id_cargo_funcao
    )
    count = session.exec(query).one()
    return count > 0


def listar_com_relacionamentos(
    session: Session,
    limit: int = 50,
    offset: int = 0
) -> List[FuncaoCargo]:
    """
    Lista funções/cargos com seus relacionamentos carregados.
    
    Args:
        session: Sessão do banco de dados
        limit: Limite de resultados
        offset: Offset para paginação
        
    Returns:
        Lista de FuncaoCargo com relacionamentos
    """
    query = (
        select(FuncaoCargo)
        .offset(offset)
        .limit(limit)
    )
    result = session.exec(query).all()
    
    # Force o carregamento dos relacionamentos
    for funcao_cargo in result:
        _ = funcao_cargo.servidor
        _ = funcao_cargo.cargo_funcao
    
    return result


def deletar_por_servidor(session: Session, id_servidor: int) -> int:
    """
    Deleta todas as funções/cargos de um servidor específico.
    
    Args:
        session: Sessão do banco de dados
        id_servidor: ID do servidor
        
    Returns:
        Número de registros deletados
    """
    query = select(FuncaoCargo).where(FuncaoCargo.id_servidor == id_servidor)
    funcoes_cargos = session.exec(query).all()
    
    count = len(funcoes_cargos)
    for funcao_cargo in funcoes_cargos:
        session.delete(funcao_cargo)
    
    session.commit()
    return count