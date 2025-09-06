from typing import TYPE_CHECKING, Optional
from sqlmodel import SQLModel, Field, Relationship
from sqlalchemy import Column, Integer, Numeric, ForeignKey

if TYPE_CHECKING:
    from app.models.servidor import Servidor


class Remuneracao(SQLModel, table=True):
    __tablename__ = "remuneracoes"

    id_remuneracao: Optional[int] = Field(
        default=None,
        sa_column=Column("id_remuneracao", Integer, primary_key=True, autoincrement=True)
    )

    id_servidor: int = Field(
        foreign_key="servidores.id_servidor",
        nullable=False,
        index=True
    )

    mes: int
    ano: int

    remuneracao: float = Field(sa_column=Column("remuneracao", Numeric(10, 2)))
    irrf: float = Field(sa_column=Column("irrf", Numeric(10, 2)))
    pss_rpgs: float = Field(sa_column=Column("pss_rpgs", Numeric(10, 2)))
    remuneracao_final: float = Field(sa_column=Column("remuneracao_final", Numeric(10, 2)))

    servidor: Optional["Servidor"] = Relationship(back_populates="remuneracoes")