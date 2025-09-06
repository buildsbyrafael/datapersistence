from typing import TYPE_CHECKING, Optional, List
from sqlmodel import SQLModel, Field, Relationship
from sqlalchemy import Column, BigInteger

if TYPE_CHECKING:
    from app.models.funcaocargo import FuncaoCargo
    from app.models.observacao import Observacao
    from app.models.afastamento import Afastamento
    from app.models.remuneracao import Remuneracao
    
    
class Servidor(SQLModel, table=True):
    __tablename__ = "servidores"

    id_servidor: Optional[int] = Field(
        default=None,
        sa_column=Column("id_servidor", BigInteger, primary_key=True, index=True)
    )
    nome: str
    cpf: str
    descr_cargo: str
    org_superior: str
    org_exercicio: str
    regime: str
    jornada_trabalho: str

    remuneracoes: List["Remuneracao"] = Relationship(back_populates="servidor")
    afastamentos: List["Afastamento"] = Relationship(back_populates="servidor")
    observacoes: List["Observacao"] = Relationship(back_populates="servidor")
    funcoes_cargos: List["FuncaoCargo"] = Relationship(back_populates="servidor")