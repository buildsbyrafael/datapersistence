from typing import Optional, List
from sqlmodel import Session, select
from app.models.servidor import Servidor
from sqlalchemy import func


def criar_servidor(session: Session, servidor: Servidor) -> Servidor:
    session.add(servidor)
    session.commit()
    session.refresh(servidor)
    return servidor


def buscar_por_id(session: Session, id_servidor: int) -> Optional[Servidor]:
    return session.get(Servidor, id_servidor)


def listar_todos(session: Session, limit: int = 50, offset: int = 0) -> List[Servidor]:
    query = select(Servidor).offset(offset).limit(limit)
    return session.exec(query).all()


def buscar_com_filtros(
    session: Session,
    nome: Optional[str] = None,
    org_exercicio: Optional[str] = None,
    cpf_parcial: Optional[str] = None,
    descr_cargo: Optional[str] = None,
    org_superior: Optional[str] = None,
    regime: Optional[str] = None,
    jornada_trabalho: Optional[str] = None,
    limit: int = 50,
    offset: int = 0
) -> List[Servidor]:

    query = select(Servidor)

    if nome:
        query = query.where(func.lower(Servidor.nome).like(f"%{nome.lower()}%"))
    if org_exercicio:
        query = query.where(func.lower(Servidor.org_exercicio).like(f"%{org_exercicio.lower()}%"))
    if cpf_parcial:
        query = query.where(Servidor.cpf.ilike(f"%{cpf_parcial}%"))
    if descr_cargo:
        query = query.where(func.lower(Servidor.descr_cargo).like(f"%{descr_cargo.lower()}%"))
    if org_superior:
        query = query.where(func.lower(Servidor.org_superior).like(f"%{org_superior.lower()}%"))
    if regime:
        query = query.where(func.lower(Servidor.regime).like(f"%{regime.lower()}%"))
    if jornada_trabalho:
        query = query.where(func.lower(Servidor.jornada_trabalho).like(f"%{jornada_trabalho.lower()}%"))

    query = query.offset(offset).limit(limit)
    return session.exec(query).all()


def contar_com_filtros(
    session: Session,
    nome: Optional[str] = None,
    org_exercicio: Optional[str] = None,
    cpf_parcial: Optional[str] = None,
    descr_cargo: Optional[str] = None,
    org_superior: Optional[str] = None,
    regime: Optional[str] = None,
    jornada_trabalho: Optional[str] = None
) -> int:

    query = select(func.count()).select_from(Servidor)

    if nome:
        query = query.where(func.lower(Servidor.nome).like(f"%{nome.lower()}%"))
    if org_exercicio:
        query = query.where(func.lower(Servidor.org_exercicio).like(f"%{org_exercicio.lower()}%"))
    if cpf_parcial:
        query = query.where(Servidor.cpf.ilike(f"%{cpf_parcial}%"))
    if descr_cargo:
        query = query.where(func.lower(Servidor.descr_cargo).like(f"%{descr_cargo.lower()}%"))
    if org_superior:
        query = query.where(func.lower(Servidor.org_superior).like(f"%{org_superior.lower()}%"))
    if regime:
        query = query.where(func.lower(Servidor.regime).like(f"%{regime.lower()}%"))
    if jornada_trabalho:
        query = query.where(func.lower(Servidor.jornada_trabalho).like(f"%{jornada_trabalho.lower()}%"))

    return session.exec(query).one()


# ========== OPERAÇÕES UPDATE ==========

def atualizar_servidor(
    session: Session, 
    id_servidor: int, 
    dados_atualizacao: dict
) -> Optional[Servidor]:
    """
    Atualiza um servidor existente com os dados fornecidos.
    
    Args:
        session: Sessão do banco de dados
        id_servidor: ID do servidor a ser atualizado
        dados_atualizacao: Dicionário com os campos a serem atualizados
        
    Returns:
        Servidor atualizado ou None se não encontrado
    """
    servidor = session.get(Servidor, id_servidor)
    if not servidor:
        return None
    
    for campo, valor in dados_atualizacao.items():
        if hasattr(servidor, campo) and valor is not None:
            setattr(servidor, campo, valor)
    
    session.add(servidor)
    session.commit()
    session.refresh(servidor)
    return servidor


def atualizar_servidor_completo(
    session: Session,
    id_servidor: int,
    nome: Optional[str] = None,
    cpf: Optional[str] = None,
    descr_cargo: Optional[str] = None,
    org_superior: Optional[str] = None,
    org_exercicio: Optional[str] = None,
    regime: Optional[str] = None,
    jornada_trabalho: Optional[str] = None
) -> Optional[Servidor]:
    """
    Atualiza campos específicos de um servidor.
    
    Args:
        session: Sessão do banco de dados
        id_servidor: ID do servidor a ser atualizado
        nome: Novo nome (opcional)
        cpf: Novo CPF (opcional)
        descr_cargo: Nova descrição do cargo (opcional)
        org_superior: Nova organização superior (opcional)
        org_exercicio: Nova organização de exercício (opcional)
        regime: Novo regime (opcional)
        jornada_trabalho: Nova jornada de trabalho (opcional)
        
    Returns:
        Servidor atualizado ou None se não encontrado
    """
    servidor = session.get(Servidor, id_servidor)
    if not servidor:
        return None
    
    if nome is not None:
        servidor.nome = nome
    if cpf is not None:
        servidor.cpf = cpf
    if descr_cargo is not None:
        servidor.descr_cargo = descr_cargo
    if org_superior is not None:
        servidor.org_superior = org_superior
    if org_exercicio is not None:
        servidor.org_exercicio = org_exercicio
    if regime is not None:
        servidor.regime = regime
    if jornada_trabalho is not None:
        servidor.jornada_trabalho = jornada_trabalho
    
    session.add(servidor)
    session.commit()
    session.refresh(servidor)
    return servidor


# ========== OPERAÇÕES DELETE ==========

def deletar_servidor(session: Session, id_servidor: int) -> bool:
    """
    Deleta um servidor pelo ID.
    
    Args:
        session: Sessão do banco de dados
        id_servidor: ID do servidor a ser deletado
        
    Returns:
        True se deletado com sucesso, False se não encontrado
    """
    servidor = session.get(Servidor, id_servidor)
    if not servidor:
        return False
    
    session.delete(servidor)
    session.commit()
    return True


def deletar_servidores_em_lote(session: Session, ids_servidores: List[int]) -> int:
    """
    Deleta múltiplos servidores pelos IDs.
    
    Args:
        session: Sessão do banco de dados
        ids_servidores: Lista de IDs dos servidores a serem deletados
        
    Returns:
        Número de servidores deletados
    """
    query = select(Servidor).where(Servidor.id_servidor.in_(ids_servidores))
    servidores = session.exec(query).all()
    
    count_deletados = len(servidores)
    
    for servidor in servidores:
        session.delete(servidor)
    
    session.commit()
    return count_deletados


def deletar_com_filtros(
    session: Session,
    nome: Optional[str] = None,
    org_exercicio: Optional[str] = None,
    cpf_parcial: Optional[str] = None,
    descr_cargo: Optional[str] = None,
    org_superior: Optional[str] = None,
    regime: Optional[str] = None,
    jornada_trabalho: Optional[str] = None,
    confirmar: bool = False
) -> int:
    """
    Deleta servidores com base em filtros específicos.
    CUIDADO: Esta operação pode deletar múltiplos registros!
    
    Args:
        session: Sessão do banco de dados
        nome: Filtro por nome (opcional)
        org_exercicio: Filtro por organização de exercício (opcional)
        cpf_parcial: Filtro por CPF parcial (opcional)
        descr_cargo: Filtro por descrição do cargo (opcional)
        org_superior: Filtro por organização superior (opcional)
        regime: Filtro por regime (opcional)
        jornada_trabalho: Filtro por jornada de trabalho (opcional)
        confirmar: Flag de confirmação para evitar deleções acidentais
        
    Returns:
        Número de servidores deletados
        
    Raises:
        ValueError: Se confirmar=False (proteção contra deleção acidental)
    """
    if not confirmar:
        raise ValueError("Para deletar com filtros, é necessário confirmar=True para evitar deleções acidentais")
    
    query = select(Servidor)
    
    if nome:
        query = query.where(func.lower(Servidor.nome).like(f"%{nome.lower()}%"))
    if org_exercicio:
        query = query.where(func.lower(Servidor.org_exercicio).like(f"%{org_exercicio.lower()}%"))
    if cpf_parcial:
        query = query.where(Servidor.cpf.ilike(f"%{cpf_parcial}%"))
    if descr_cargo:
        query = query.where(func.lower(Servidor.descr_cargo).like(f"%{descr_cargo.lower()}%"))
    if org_superior:
        query = query.where(func.lower(Servidor.org_superior).like(f"%{org_superior.lower()}%"))
    if regime:
        query = query.where(func.lower(Servidor.regime).like(f"%{regime.lower()}%"))
    if jornada_trabalho:
        query = query.where(func.lower(Servidor.jornada_trabalho).like(f"%{jornada_trabalho.lower()}%"))
    
    servidores = session.exec(query).all()
    count_deletados = len(servidores)
    
    for servidor in servidores:
        session.delete(servidor)
    
    session.commit()
    return count_deletados


# ========== OPERAÇÕES AUXILIARES ==========

def verificar_cpf_existe(session: Session, cpf: str, excluir_id: Optional[int] = None) -> bool:
    """
    Verifica se um CPF já existe no banco de dados.
    
    Args:
        session: Sessão do banco de dados
        cpf: CPF a ser verificado
        excluir_id: ID do servidor a ser excluído da verificação (útil para updates)
        
    Returns:
        True se o CPF já existe, False caso contrário
    """
    query = select(Servidor).where(Servidor.cpf == cpf)
    
    if excluir_id:
        query = query.where(Servidor.id_servidor != excluir_id)
    
    servidor = session.exec(query).first()
    return servidor is not None


def buscar_por_cpf(session: Session, cpf: str) -> Optional[Servidor]:
    """
    Busca um servidor pelo CPF.
    
    Args:
        session: Sessão do banco de dados
        cpf: CPF do servidor
        
    Returns:
        Servidor encontrado ou None
    """
    query = select(Servidor).where(Servidor.cpf == cpf)
    return session.exec(query).first()