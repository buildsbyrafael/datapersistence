from typing import Optional, List, Dict, Any
from sqlmodel import Session, select
from sqlalchemy import func, and_, or_
from app.models.afastamento import Afastamento


def criar_afastamento(session: Session, afastamento: Afastamento) -> Afastamento:
    session.add(afastamento)
    session.commit()
    session.refresh(afastamento)
    return afastamento


def buscar_por_id(session: Session, id_afastamento: int) -> Optional[Afastamento]:
    return session.get(Afastamento, id_afastamento)


def atualizar_afastamento(
    session: Session, 
    id_afastamento: int, 
    dados_atualizacao: Dict[str, Any]
) -> Optional[Afastamento]:
    """Atualiza um afastamento específico com os dados fornecidos"""
    afastamento = session.get(Afastamento, id_afastamento)
    if not afastamento:
        return None
    
    for campo, valor in dados_atualizacao.items():
        if hasattr(afastamento, campo):
            setattr(afastamento, campo, valor)
    
    session.add(afastamento)
    session.commit()
    session.refresh(afastamento)
    return afastamento


def atualizar_afastamento_completo(
    session: Session,
    id_afastamento: int,
    afastamento_atualizado: Afastamento
) -> Optional[Afastamento]:
    """Atualiza um afastamento substituindo todos os campos"""
    afastamento_existente = session.get(Afastamento, id_afastamento)
    if not afastamento_existente:
        return None
    
    afastamento_existente.id_servidor = afastamento_atualizado.id_servidor
    afastamento_existente.mes = afastamento_atualizado.mes
    afastamento_existente.ano = afastamento_atualizado.ano
    afastamento_existente.inicio_afastamento = afastamento_atualizado.inicio_afastamento
    afastamento_existente.duracao_dias = afastamento_atualizado.duracao_dias
    
    session.add(afastamento_existente)
    session.commit()
    session.refresh(afastamento_existente)
    return afastamento_existente


def deletar_afastamento(session: Session, id_afastamento: int) -> bool:
    """Deleta um afastamento específico"""
    afastamento = session.get(Afastamento, id_afastamento)
    if not afastamento:
        return False
    
    session.delete(afastamento)
    session.commit()
    return True


def listar_todos(
    session: Session,
    limit: int = 50,
    offset: int = 0
) -> List[Afastamento]:
    """Lista todos os afastamentos com paginação"""
    query = select(Afastamento).offset(offset).limit(limit)
    return session.exec(query).all()


def listar_por_servidor(
    session: Session,
    id_servidor: int,
    ano: Optional[int] = None,
    mes: Optional[int] = None,
    limit: int = 50,
    offset: int = 0
) -> List[Afastamento]:
    query = select(Afastamento).where(Afastamento.id_servidor == id_servidor)

    if ano:
        query = query.where(Afastamento.ano == ano)
    if mes:
        query = query.where(Afastamento.mes == mes)

    query = query.offset(offset).limit(limit)
    return session.exec(query).all()


def buscar_por_mes_ano(
    session: Session,
    ano: int,
    mes: int,
    limit: int = 50,
    offset: int = 0
) -> List[Afastamento]:
    query = (
        select(Afastamento)
        .where(Afastamento.ano == ano, Afastamento.mes == mes)
        .offset(offset)
        .limit(limit)
    )
    return session.exec(query).all()


def buscar_por_periodo(
    session: Session,
    ano_inicio: int,
    mes_inicio: int,
    ano_fim: int,
    mes_fim: int,
    limit: int = 50,
    offset: int = 0
) -> List[Afastamento]:
    """Busca afastamentos em um período específico"""
    query = select(Afastamento).where(
        or_(
            and_(Afastamento.ano > ano_inicio),
            and_(Afastamento.ano == ano_inicio, Afastamento.mes >= mes_inicio)
        ),
        or_(
            and_(Afastamento.ano < ano_fim),
            and_(Afastamento.ano == ano_fim, Afastamento.mes <= mes_fim)
        )
    ).offset(offset).limit(limit)
    
    return session.exec(query).all()


def buscar_por_duracao_minima(
    session: Session,
    duracao_minima: int,
    limit: int = 50,
    offset: int = 0
) -> List[Afastamento]:
    """Busca afastamentos com duração mínima especificada"""
    query = (
        select(Afastamento)
        .where(Afastamento.duracao_dias >= duracao_minima)
        .offset(offset)
        .limit(limit)
    )
    return session.exec(query).all()


def contar_afastamentos_filtrados(
    session: Session,
    ano: int,
    mes: int
) -> int:
    query = (
        select(func.count())
        .select_from(Afastamento)
        .where(Afastamento.ano == ano, Afastamento.mes == mes)
    )
    return session.exec(query).one()


def contar_total_afastamentos(session: Session) -> int:
    """Conta o total de afastamentos na base"""
    query = select(func.count()).select_from(Afastamento)
    return session.exec(query).one()


def contar_afastamentos_por_servidor(
    session: Session,
    id_servidor: int,
    ano: Optional[int] = None,
    mes: Optional[int] = None
) -> int:
    """Conta afastamentos de um servidor específico"""
    query = (
        select(func.count())
        .select_from(Afastamento)
        .where(Afastamento.id_servidor == id_servidor)
    )
    
    if ano:
        query = query.where(Afastamento.ano == ano)
    if mes:
        query = query.where(Afastamento.mes == mes)
    
    return session.exec(query).one()


def buscar_afastamentos_longos(
    session: Session,
    limite_dias: int = 30,
    limit: int = 50,
    offset: int = 0
) -> List[Afastamento]:
    """Busca afastamentos considerados longos (acima do limite de dias)"""
    query = (
        select(Afastamento)
        .where(Afastamento.duracao_dias > limite_dias)
        .offset(offset)
        .limit(limit)
    )
    return session.exec(query).all()


def deletar_afastamentos_por_servidor(
    session: Session,
    id_servidor: int,
    ano: Optional[int] = None,
    mes: Optional[int] = None
) -> int:
    """Deleta afastamentos de um servidor específico e retorna a quantidade deletada"""
    query = select(Afastamento).where(Afastamento.id_servidor == id_servidor)
    
    if ano:
        query = query.where(Afastamento.ano == ano)
    if mes:
        query = query.where(Afastamento.mes == mes)
    
    afastamentos = session.exec(query).all()
    quantidade_deletada = len(afastamentos)
    
    for afastamento in afastamentos:
        session.delete(afastamento)
    
    session.commit()
    return quantidade_deletada


def obter_estatisticas_servidor(
    session: Session,
    id_servidor: int,
    ano: Optional[int] = None
) -> Dict[str, Any]:
    """Obtém estatísticas de afastamentos de um servidor"""
    query = (
        select(
            func.count(Afastamento.id_afastamento).label('total_afastamentos'),
            func.sum(Afastamento.duracao_dias).label('total_dias'),
            func.avg(Afastamento.duracao_dias).label('media_dias'),
            func.max(Afastamento.duracao_dias).label('maior_afastamento'),
            func.min(Afastamento.duracao_dias).label('menor_afastamento')
        )
        .where(Afastamento.id_servidor == id_servidor)
    )
    
    if ano:
        query = query.where(Afastamento.ano == ano)
    
    resultado = session.exec(query).first()
    
    return {
        'total_afastamentos': resultado.total_afastamentos or 0,
        'total_dias': resultado.total_dias or 0,
        'media_dias': float(resultado.media_dias) if resultado.media_dias else 0,
        'maior_afastamento': resultado.maior_afastamento or 0,
        'menor_afastamento': resultado.menor_afastamento or 0
    }