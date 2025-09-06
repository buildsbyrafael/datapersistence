from typing import Optional, List
from sqlmodel import Session, select
from sqlalchemy import func
from app.models.observacao import Observacao


def criar_observacao(session: Session, observacao: Observacao) -> Observacao:
    session.add(observacao)
    session.commit()
    session.refresh(observacao)
    return observacao


def buscar_por_id(session: Session, id_observacao: int) -> Optional[Observacao]:
    return session.get(Observacao, id_observacao)


def listar_por_servidor(
    session: Session,
    id_servidor: int,
    ano: Optional[int] = None,
    mes: Optional[int] = None,
    limit: int = 50,
    offset: int = 0
) -> List[Observacao]:
    query = select(Observacao).where(Observacao.id_servidor == id_servidor)

    if ano:
        query = query.where(Observacao.ano == ano)
    if mes:
        query = query.where(Observacao.mes == mes)

    query = query.offset(offset).limit(limit)
    return session.exec(query).all()


def buscar_por_mes_ano(
    session: Session,
    ano: int,
    mes: int,
    limit: int = 50,
    offset: int = 0
) -> List[Observacao]:
    query = (
        select(Observacao)
        .where(Observacao.ano == ano, Observacao.mes == mes)
        .offset(offset)
        .limit(limit)
    )
    return session.exec(query).all()


def contar_observacoes_filtradas(
    session: Session,
    ano: int,
    mes: int
) -> int:
    query = (
        select(func.count())
        .select_from(Observacao)
        .where(Observacao.ano == ano, Observacao.mes == mes)
    )
    return session.exec(query).one()


# OPERAÇÕES CRUD ADICIONAIS

def listar_todas(
    session: Session,
    limit: int = 50,
    offset: int = 0
) -> List[Observacao]:
    """Lista todas as observações com paginação"""
    query = select(Observacao).offset(offset).limit(limit)
    return session.exec(query).all()


def atualizar_observacao(
    session: Session,
    id_observacao: int,
    dados_atualizacao: dict
) -> Optional[Observacao]:
    """Atualiza uma observação existente"""
    observacao = session.get(Observacao, id_observacao)
    if not observacao:
        return None
    
    for campo, valor in dados_atualizacao.items():
        if hasattr(observacao, campo):
            setattr(observacao, campo, valor)
    
    session.add(observacao)
    session.commit()
    session.refresh(observacao)
    return observacao


def deletar_observacao(session: Session, id_observacao: int) -> bool:
    """Deleta uma observação pelo ID"""
    observacao = session.get(Observacao, id_observacao)
    if not observacao:
        return False
    
    session.delete(observacao)
    session.commit()
    return True


def deletar_observacoes_por_servidor(
    session: Session, 
    id_servidor: int,
    ano: Optional[int] = None,
    mes: Optional[int] = None
) -> int:
    """Deleta observações de um servidor, opcionalmente filtradas por ano/mês"""
    query = select(Observacao).where(Observacao.id_servidor == id_servidor)
    
    if ano:
        query = query.where(Observacao.ano == ano)
    if mes:
        query = query.where(Observacao.mes == mes)
    
    observacoes = session.exec(query).all()
    count = len(observacoes)
    
    for observacao in observacoes:
        session.delete(observacao)
    
    session.commit()
    return count


def contar_total(session: Session) -> int:
    """Conta o total de observações"""
    query = select(func.count()).select_from(Observacao)
    return session.exec(query).one()


def contar_por_servidor(
    session: Session,
    id_servidor: int,
    ano: Optional[int] = None,
    mes: Optional[int] = None
) -> int:
    """Conta observações de um servidor específico"""
    query = (
        select(func.count())
        .select_from(Observacao)
        .where(Observacao.id_servidor == id_servidor)
    )
    
    if ano:
        query = query.where(Observacao.ano == ano)
    if mes:
        query = query.where(Observacao.mes == mes)
    
    return session.exec(query).one()


def buscar_por_flag_teto(
    session: Session,
    flag_teto: bool,
    ano: Optional[int] = None,
    mes: Optional[int] = None,
    limit: int = 50,
    offset: int = 0
) -> List[Observacao]:
    """Busca observações pela flag_teto"""
    query = select(Observacao).where(Observacao.flag_teto == flag_teto)
    
    if ano:
        query = query.where(Observacao.ano == ano)
    if mes:
        query = query.where(Observacao.mes == mes)
    
    query = query.offset(offset).limit(limit)
    return session.exec(query).all()


def buscar_por_conteudo(
    session: Session,
    termo_busca: str,
    limit: int = 50,
    offset: int = 0
) -> List[Observacao]:
    """Busca observações por conteúdo do texto (case-insensitive)"""
    query = (
        select(Observacao)
        .where(Observacao.observacao.ilike(f"%{termo_busca}%"))
        .offset(offset)
        .limit(limit)
    )
    return session.exec(query).all()


def existe_observacao(session: Session, id_observacao: int) -> bool:
    """Verifica se uma observação existe"""
    query = select(func.count()).select_from(Observacao).where(Observacao.id_observacao == id_observacao)
    count = session.exec(query).one()
    return count > 0


def atualizar_parcial(
    session: Session,
    id_observacao: int,
    **kwargs
) -> Optional[Observacao]:
    """Atualização parcial usando kwargs"""
    observacao = session.get(Observacao, id_observacao)
    if not observacao:
        return None
    
    for campo, valor in kwargs.items():
        if hasattr(observacao, campo) and valor is not None:
            setattr(observacao, campo, valor)
    
    session.add(observacao)
    session.commit()
    session.refresh(observacao)
    return observacao