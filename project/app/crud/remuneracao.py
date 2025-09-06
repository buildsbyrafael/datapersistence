from typing import Optional, List
from sqlalchemy import func
from sqlmodel import Session, select
from app.models.remuneracao import Remuneracao


# CREATE - Criar nova remuneração
def criar_remuneracao(
    session: Session,
    id_servidor: int,
    mes: int,
    ano: int,
    remuneracao: float,
    irrf: float,
    pss_rpgs: float,
    remuneracao_final: float
) -> Remuneracao:
    """Cria uma nova remuneração no banco de dados."""
    nova_remuneracao = Remuneracao(
        id_servidor=id_servidor,
        mes=mes,
        ano=ano,
        remuneracao=remuneracao,
        irrf=irrf,
        pss_rpgs=pss_rpgs,
        remuneracao_final=remuneracao_final
    )
    
    session.add(nova_remuneracao)
    session.commit()
    session.refresh(nova_remuneracao)
    return nova_remuneracao


def criar_remuneracao_from_dict(
    session: Session,
    dados: dict
) -> Remuneracao:
    """Cria uma nova remuneração a partir de um dicionário."""
    nova_remuneracao = Remuneracao(**dados)
    session.add(nova_remuneracao)
    session.commit()
    session.refresh(nova_remuneracao)
    return nova_remuneracao


# READ - Buscar remunerações
def buscar_remuneracao_por_id(
    session: Session,
    id_remuneracao: int
) -> Optional[Remuneracao]:
    """Busca uma remuneração pelo ID."""
    return session.get(Remuneracao, id_remuneracao)


def buscar_por_mes_ano(
    session: Session,
    ano: int,
    mes: int,
    limit: int = 50,
    offset: int = 0
) -> List[Remuneracao]:
    """Busca remunerações por mês e ano."""
    query = (
        select(Remuneracao)
        .where(Remuneracao.ano == ano, Remuneracao.mes == mes)
        .offset(offset)
        .limit(limit)
    )
    return session.exec(query).all()


def listar_por_servidor(
    session: Session,
    id_servidor: int,
    ano: Optional[int] = None,
    mes: Optional[int] = None,
    limit: int = 50,
    offset: int = 0
) -> List[Remuneracao]:
    """Lista remunerações de um servidor específico."""
    query = select(Remuneracao).where(Remuneracao.id_servidor == id_servidor)

    if ano:
        query = query.where(Remuneracao.ano == ano)
    if mes:
        query = query.where(Remuneracao.mes == mes)

    query = query.offset(offset).limit(limit)
    return session.exec(query).all()


def listar_todas_remuneracoes(
    session: Session,
    limit: int = 100,
    offset: int = 0
) -> List[Remuneracao]:
    """Lista todas as remunerações com paginação."""
    query = select(Remuneracao).offset(offset).limit(limit)
    return session.exec(query).all()


def buscar_remuneracoes_filtradas(
    session: Session,
    ano: int,
    mes: int,
    remuneracao_min: Optional[float] = None,
    remuneracao_max: Optional[float] = None,
    irrf_min: Optional[float] = None,
    irrf_max: Optional[float] = None,
    pss_rpgs_min: Optional[float] = None,
    pss_rpgs_max: Optional[float] = None,
    remuneracao_final_min: Optional[float] = None,
    remuneracao_final_max: Optional[float] = None,
    limit: int = 50,
    offset: int = 0
) -> List[Remuneracao]:
    """Busca remunerações com filtros específicos."""
    query = select(Remuneracao).where(
        Remuneracao.ano == ano,
        Remuneracao.mes == mes
    )

    if remuneracao_min is not None:
        query = query.where(Remuneracao.remuneracao >= remuneracao_min)
    if remuneracao_max is not None:
        query = query.where(Remuneracao.remuneracao <= remuneracao_max)

    if irrf_min is not None:
        query = query.where(Remuneracao.irrf >= irrf_min)
    if irrf_max is not None:
        query = query.where(Remuneracao.irrf <= irrf_max)

    if pss_rpgs_min is not None:
        query = query.where(Remuneracao.pss_rpgs >= pss_rpgs_min)
    if pss_rpgs_max is not None:
        query = query.where(Remuneracao.pss_rpgs <= pss_rpgs_max)

    if remuneracao_final_min is not None:
        query = query.where(Remuneracao.remuneracao_final >= remuneracao_final_min)
    if remuneracao_final_max is not None:
        query = query.where(Remuneracao.remuneracao_final <= remuneracao_final_max)

    query = query.offset(offset).limit(limit)
    return session.exec(query).all()


def contar_remuneracoes_filtradas(
    session: Session,
    ano: int,
    mes: int,
    remuneracao_min: Optional[float] = None,
    remuneracao_max: Optional[float] = None,
    irrf_min: Optional[float] = None,
    irrf_max: Optional[float] = None,
    pss_rpgs_min: Optional[float] = None,
    pss_rpgs_max: Optional[float] = None,
    remuneracao_final_min: Optional[float] = None,
    remuneracao_final_max: Optional[float] = None,
) -> int:
    """Conta remunerações com filtros específicos."""
    query = select(func.count()).select_from(Remuneracao).where(
        Remuneracao.ano == ano,
        Remuneracao.mes == mes
    )

    if remuneracao_min is not None:
        query = query.where(Remuneracao.remuneracao >= remuneracao_min)
    if remuneracao_max is not None:
        query = query.where(Remuneracao.remuneracao <= remuneracao_max)

    if irrf_min is not None:
        query = query.where(Remuneracao.irrf >= irrf_min)
    if irrf_max is not None:
        query = query.where(Remuneracao.irrf <= irrf_max)

    if pss_rpgs_min is not None:
        query = query.where(Remuneracao.pss_rpgs >= pss_rpgs_min)
    if pss_rpgs_max is not None:
        query = query.where(Remuneracao.pss_rpgs <= pss_rpgs_max)

    if remuneracao_final_min is not None:
        query = query.where(Remuneracao.remuneracao_final >= remuneracao_final_min)
    if remuneracao_final_max is not None:
        query = query.where(Remuneracao.remuneracao_final <= remuneracao_final_max)

    return session.exec(query).one()


# UPDATE - Atualizar remunerações
def atualizar_remuneracao(
    session: Session,
    id_remuneracao: int,
    dados_atualizacao: dict
) -> Optional[Remuneracao]:
    """Atualiza uma remuneração existente."""
    remuneracao = session.get(Remuneracao, id_remuneracao)
    if not remuneracao:
        return None
    
    for campo, valor in dados_atualizacao.items():
        if hasattr(remuneracao, campo):
            setattr(remuneracao, campo, valor)
    
    session.add(remuneracao)
    session.commit()
    session.refresh(remuneracao)
    return remuneracao


def atualizar_remuneracao_completa(
    session: Session,
    id_remuneracao: int,
    id_servidor: Optional[int] = None,
    mes: Optional[int] = None,
    ano: Optional[int] = None,
    remuneracao: Optional[float] = None,
    irrf: Optional[float] = None,
    pss_rpgs: Optional[float] = None,
    remuneracao_final: Optional[float] = None
) -> Optional[Remuneracao]:
    """Atualiza campos específicos de uma remuneração."""
    remuneracao_obj = session.get(Remuneracao, id_remuneracao)
    if not remuneracao_obj:
        return None
    
    if id_servidor is not None:
        remuneracao_obj.id_servidor = id_servidor
    if mes is not None:
        remuneracao_obj.mes = mes
    if ano is not None:
        remuneracao_obj.ano = ano
    if remuneracao is not None:
        remuneracao_obj.remuneracao = remuneracao
    if irrf is not None:
        remuneracao_obj.irrf = irrf
    if pss_rpgs is not None:
        remuneracao_obj.pss_rpgs = pss_rpgs
    if remuneracao_final is not None:
        remuneracao_obj.remuneracao_final = remuneracao_final
    
    session.add(remuneracao_obj)
    session.commit()
    session.refresh(remuneracao_obj)
    return remuneracao_obj


# DELETE - Remover remunerações
def deletar_remuneracao(
    session: Session,
    id_remuneracao: int
) -> bool:
    """Remove uma remuneração pelo ID."""
    remuneracao = session.get(Remuneracao, id_remuneracao)
    if not remuneracao:
        return False
    
    session.delete(remuneracao)
    session.commit()
    return True


def deletar_remuneracoes_por_servidor(
    session: Session,
    id_servidor: int,
    ano: Optional[int] = None,
    mes: Optional[int] = None
) -> int:
    """Remove todas as remunerações de um servidor (com filtros opcionais)."""
    query = select(Remuneracao).where(Remuneracao.id_servidor == id_servidor)
    
    if ano is not None:
        query = query.where(Remuneracao.ano == ano)
    if mes is not None:
        query = query.where(Remuneracao.mes == mes)
    
    remuneracoes = session.exec(query).all()
    count = len(remuneracoes)
    
    for remuneracao in remuneracoes:
        session.delete(remuneracao)
    
    session.commit()
    return count


def deletar_remuneracoes_por_periodo(
    session: Session,
    ano: int,
    mes: Optional[int] = None
) -> int:
    """Remove todas as remunerações de um período específico."""
    query = select(Remuneracao).where(Remuneracao.ano == ano)
    
    if mes is not None:
        query = query.where(Remuneracao.mes == mes)
    
    remuneracoes = session.exec(query).all()
    count = len(remuneracoes)
    
    for remuneracao in remuneracoes:
        session.delete(remuneracao)
    
    session.commit()
    return count


# FUNÇÕES AUXILIARES E ESTATÍSTICAS
def obter_estatisticas_remuneracao(
    session: Session,
    ano: int,
    mes: Optional[int] = None,
    id_servidor: Optional[int] = None
) -> dict:
    """Obtém estatísticas básicas das remunerações."""
    query = select(
        func.count(Remuneracao.id_remuneracao).label('total'),
        func.avg(Remuneracao.remuneracao).label('media_remuneracao'),
        func.min(Remuneracao.remuneracao).label('min_remuneracao'),
        func.max(Remuneracao.remuneracao).label('max_remuneracao'),
        func.sum(Remuneracao.remuneracao).label('total_remuneracao'),
        func.avg(Remuneracao.remuneracao_final).label('media_final'),
        func.sum(Remuneracao.irrf).label('total_irrf'),
        func.sum(Remuneracao.pss_rpgs).label('total_pss_rpgs')
    ).where(Remuneracao.ano == ano)
    
    if mes is not None:
        query = query.where(Remuneracao.mes == mes)
    if id_servidor is not None:
        query = query.where(Remuneracao.id_servidor == id_servidor)
    
    resultado = session.exec(query).first()
    
    return {
        'total_registros': resultado.total or 0,
        'media_remuneracao': float(resultado.media_remuneracao or 0),
        'min_remuneracao': float(resultado.min_remuneracao or 0),
        'max_remuneracao': float(resultado.max_remuneracao or 0),
        'total_remuneracao': float(resultado.total_remuneracao or 0),
        'media_remuneracao_final': float(resultado.media_final or 0),
        'total_irrf': float(resultado.total_irrf or 0),
        'total_pss_rpgs': float(resultado.total_pss_rpgs or 0)
    }


def verificar_duplicata(
    session: Session,
    id_servidor: int,
    ano: int,
    mes: int
) -> Optional[Remuneracao]:
    """Verifica se já existe uma remuneração para o servidor no período especificado."""
    query = select(Remuneracao).where(
        Remuneracao.id_servidor == id_servidor,
        Remuneracao.ano == ano,
        Remuneracao.mes == mes
    )
    return session.exec(query).first()


def buscar_historico_servidor(
    session: Session,
    id_servidor: int,
    ano_inicio: Optional[int] = None,
    ano_fim: Optional[int] = None,
    ordenar_por_data: bool = True
) -> List[Remuneracao]:
    """Busca o histórico completo de remunerações de um servidor."""
    query = select(Remuneracao).where(Remuneracao.id_servidor == id_servidor)
    
    if ano_inicio is not None:
        query = query.where(Remuneracao.ano >= ano_inicio)
    if ano_fim is not None:
        query = query.where(Remuneracao.ano <= ano_fim)
    
    if ordenar_por_data:
        query = query.order_by(Remuneracao.ano.desc(), Remuneracao.mes.desc())
    
    return session.exec(query).all()