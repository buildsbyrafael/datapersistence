from typing import Optional, List
from sqlmodel import Session, select
from sqlalchemy import func
from app.models.cargofuncao import CargoFuncao


def criar_cargo_funcao(session: Session, cargo_funcao: CargoFuncao) -> CargoFuncao:
    session.add(cargo_funcao)
    session.commit()
    session.refresh(cargo_funcao)
    return cargo_funcao


def buscar_por_id(session: Session, id_cargo_funcao: int) -> Optional[CargoFuncao]:
    return session.get(CargoFuncao, id_cargo_funcao)


def listar_cargosfuncoes(
    session: Session,
    classe_cargo: Optional[str] = None,
    referencia_cargo: Optional[int] = None,
    padrao_cargo: Optional[int] = None,
    nivel_cargo: Optional[int] = None,
    funcao: Optional[str] = None,
    descricao_cargo: Optional[str] = None,
    nivel_funcao: Optional[int] = None,
    limit: int = 50,
    offset: int = 0
) -> List[CargoFuncao]:
    query = select(CargoFuncao)

    if classe_cargo:
        query = query.where(CargoFuncao.classe_cargo == classe_cargo)
    if referencia_cargo:
        query = query.where(CargoFuncao.referencia_cargo == referencia_cargo)
    if padrao_cargo:
        query = query.where(CargoFuncao.padrao_cargo == padrao_cargo)
    if nivel_cargo:
        query = query.where(CargoFuncao.nivel_cargo == nivel_cargo)
    if funcao:
        query = query.where(CargoFuncao.funcao == funcao)
    if descricao_cargo:
        query = query.where(CargoFuncao.descricao_cargo == descricao_cargo)
    if nivel_funcao:
        query = query.where(CargoFuncao.nivel_funcao == nivel_funcao)

    query = query.offset(offset).limit(limit)
    return session.exec(query).all()


def buscar_duplicado(
    session: Session,
    classe_cargo: Optional[str],
    referencia_cargo: Optional[int],
    padrao_cargo: Optional[int],
    nivel_cargo: Optional[int],
    funcao: Optional[str],
    descricao_cargo: str,
    nivel_funcao: Optional[int],
) -> Optional[CargoFuncao]:
    query = select(CargoFuncao).where(
        CargoFuncao.classe_cargo == classe_cargo,
        CargoFuncao.referencia_cargo == referencia_cargo,
        CargoFuncao.padrao_cargo == padrao_cargo,
        CargoFuncao.nivel_cargo == nivel_cargo,
        CargoFuncao.funcao == funcao,
        CargoFuncao.descricao_cargo == descricao_cargo,
        CargoFuncao.nivel_funcao == nivel_funcao,
    )

    return session.exec(query).first()


def contar_cargosfuncoes_filtradas(
    session: Session,
    classe_cargo: Optional[str] = None,
    referencia_cargo: Optional[int] = None,
    padrao_cargo: Optional[int] = None,
    nivel_cargo: Optional[int] = None,
    funcao: Optional[str] = None,
    descricao_cargo: Optional[str] = None,
    nivel_funcao: Optional[int] = None
) -> int:
    query = select(func.count()).select_from(CargoFuncao)

    if classe_cargo:
        query = query.where(CargoFuncao.classe_cargo == classe_cargo)
    if referencia_cargo:
        query = query.where(CargoFuncao.referencia_cargo == referencia_cargo)
    if padrao_cargo:
        query = query.where(CargoFuncao.padrao_cargo == padrao_cargo)
    if nivel_cargo:
        query = query.where(CargoFuncao.nivel_cargo == nivel_cargo)
    if funcao:
        query = query.where(CargoFuncao.funcao == funcao)
    if descricao_cargo:
        query = query.where(CargoFuncao.descricao_cargo == descricao_cargo)
    if nivel_funcao:
        query = query.where(CargoFuncao.nivel_funcao == nivel_funcao)

    return session.exec(query).one()


# ==================== OPERAÇÕES CRUD ADICIONAIS ====================


def atualizar_cargo_funcao(
    session: Session, 
    id_cargo_funcao: int, 
    dados_atualizacao: dict
) -> Optional[CargoFuncao]:
    """
    Atualiza um CargoFuncao existente com os dados fornecidos.
    
    Args:
        session: Sessão do banco de dados
        id_cargo_funcao: ID do cargo/função a ser atualizado
        dados_atualizacao: Dicionário com os campos a serem atualizados
    
    Returns:
        CargoFuncao atualizado ou None se não encontrado
    """
    cargo_funcao = session.get(CargoFuncao, id_cargo_funcao)
    if not cargo_funcao:
        return None
    
    for campo, valor in dados_atualizacao.items():
        if hasattr(cargo_funcao, campo):
            setattr(cargo_funcao, campo, valor)
    
    session.add(cargo_funcao)
    session.commit()
    session.refresh(cargo_funcao)
    return cargo_funcao


def atualizar_cargo_funcao_completo(
    session: Session,
    id_cargo_funcao: int,
    classe_cargo: Optional[str] = None,
    referencia_cargo: Optional[int] = None,
    padrao_cargo: Optional[int] = None,
    nivel_cargo: Optional[int] = None,
    funcao: Optional[str] = None,
    descricao_cargo: Optional[str] = None,
    nivel_funcao: Optional[int] = None,
) -> Optional[CargoFuncao]:
    """
    Atualiza um CargoFuncao com todos os campos especificados.
    
    Args:
        session: Sessão do banco de dados
        id_cargo_funcao: ID do cargo/função a ser atualizado
        **kwargs: Campos a serem atualizados
    
    Returns:
        CargoFuncao atualizado ou None se não encontrado
    """
    cargo_funcao = session.get(CargoFuncao, id_cargo_funcao)
    if not cargo_funcao:
        return None
    
    if classe_cargo is not None:
        cargo_funcao.classe_cargo = classe_cargo
    if referencia_cargo is not None:
        cargo_funcao.referencia_cargo = referencia_cargo
    if padrao_cargo is not None:
        cargo_funcao.padrao_cargo = padrao_cargo
    if nivel_cargo is not None:
        cargo_funcao.nivel_cargo = nivel_cargo
    if funcao is not None:
        cargo_funcao.funcao = funcao
    if descricao_cargo is not None:
        cargo_funcao.descricao_cargo = descricao_cargo
    if nivel_funcao is not None:
        cargo_funcao.nivel_funcao = nivel_funcao
    
    session.add(cargo_funcao)
    session.commit()
    session.refresh(cargo_funcao)
    return cargo_funcao


def deletar_cargo_funcao(session: Session, id_cargo_funcao: int) -> bool:
    """
    Deleta um CargoFuncao pelo ID.
    
    Args:
        session: Sessão do banco de dados
        id_cargo_funcao: ID do cargo/função a ser deletado
    
    Returns:
        True se deletado com sucesso, False se não encontrado
    """
    cargo_funcao = session.get(CargoFuncao, id_cargo_funcao)
    if not cargo_funcao:
        return False
    
    session.delete(cargo_funcao)
    session.commit()
    return True


def deletar_multiplos_cargosfuncoes(session: Session, ids: List[int]) -> int:
    """
    Deleta múltiplos CargosFuncoes pelos IDs.
    
    Args:
        session: Sessão do banco de dados
        ids: Lista de IDs dos cargos/funções a serem deletados
    
    Returns:
        Número de registros deletados
    """
    query = select(CargoFuncao).where(CargoFuncao.id_cargo_funcao.in_(ids))
    cargos_funcoes = session.exec(query).all()
    
    deletados = 0
    for cargo_funcao in cargos_funcoes:
        session.delete(cargo_funcao)
        deletados += 1
    
    session.commit()
    return deletados


def verificar_existencia(session: Session, id_cargo_funcao: int) -> bool:
    """
    Verifica se um CargoFuncao existe pelo ID.
    
    Args:
        session: Sessão do banco de dados
        id_cargo_funcao: ID do cargo/função a ser verificado
    
    Returns:
        True se existe, False caso contrário
    """
    query = select(func.count()).select_from(CargoFuncao).where(
        CargoFuncao.id_cargo_funcao == id_cargo_funcao
    )
    count = session.exec(query).one()
    return count > 0


def buscar_por_descricao_like(
    session: Session, 
    descricao_parcial: str,
    limit: int = 50,
    offset: int = 0
) -> List[CargoFuncao]:
    """
    Busca CargosFuncoes que contenham a descrição parcial (case-insensitive).
    
    Args:
        session: Sessão do banco de dados
        descricao_parcial: Parte da descrição a ser buscada
        limit: Limite de resultados
        offset: Offset para paginação
    
    Returns:
        Lista de CargosFuncoes encontrados
    """
    query = select(CargoFuncao).where(
        CargoFuncao.descricao_cargo.ilike(f"%{descricao_parcial}%")
    ).offset(offset).limit(limit)
    
    return session.exec(query).all()


def contar_total_cargosfuncoes(session: Session) -> int:
    """
    Conta o total de CargosFuncoes na tabela.
    
    Args:
        session: Sessão do banco de dados
    
    Returns:
        Número total de registros
    """
    query = select(func.count()).select_from(CargoFuncao)
    return session.exec(query).one()


def listar_classes_cargo_distintas(session: Session) -> List[str]:
    """
    Lista todas as classes de cargo distintas.
    
    Args:
        session: Sessão do banco de dados
    
    Returns:
        Lista de classes de cargo únicas
    """
    query = select(CargoFuncao.classe_cargo).distinct().where(
        CargoFuncao.classe_cargo.is_not(None)
    )
    return session.exec(query).all()


def listar_funcoes_distintas(session: Session) -> List[str]:
    """
    Lista todas as funções distintas.
    
    Args:
        session: Sessão do banco de dados
    
    Returns:
        Lista de funções únicas
    """
    query = select(CargoFuncao.funcao).distinct().where(
        CargoFuncao.funcao.is_not(None)
    )
    return session.exec(query).all()


def buscar_por_multiplos_ids(session: Session, ids: List[int]) -> List[CargoFuncao]:
    """
    Busca múltiplos CargosFuncoes pelos IDs.
    
    Args:
        session: Sessão do banco de dados
        ids: Lista de IDs a serem buscados
    
    Returns:
        Lista de CargosFuncoes encontrados
    """
    query = select(CargoFuncao).where(CargoFuncao.id_cargo_funcao.in_(ids))
    return session.exec(query).all()


def listar_por_nivel_cargo_range(
    session: Session,
    nivel_min: int,
    nivel_max: int,
    limit: int = 50,
    offset: int = 0
) -> List[CargoFuncao]:
    """
    Lista CargosFuncoes dentro de um intervalo de níveis de cargo.
    
    Args:
        session: Sessão do banco de dados
        nivel_min: Nível mínimo (inclusivo)
        nivel_max: Nível máximo (inclusivo)
        limit: Limite de resultados
        offset: Offset para paginação
    
    Returns:
        Lista de CargosFuncoes no intervalo especificado
    """
    query = select(CargoFuncao).where(
        CargoFuncao.nivel_cargo >= nivel_min,
        CargoFuncao.nivel_cargo <= nivel_max
    ).offset(offset).limit(limit)
    
    return session.exec(query).all()