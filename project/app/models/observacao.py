from typing import TYPE_CHECKING, Optional
from sqlmodel import SQLModel, Field, Relationship
from sqlalchemy import Column, Integer, Boolean, ForeignKey, Text

if TYPE_CHECKING:
    from app.models.servidor import Servidor


class Observacao(SQLModel, table=True):
    __tablename__ = "observacoes"

    id_observacao: Optional[int] = Field(
        default=None,
        sa_column=Column("id_observacao", Integer, primary_key=True, autoincrement=True)
    )

    id_servidor: int = Field(
        foreign_key="servidores.id_servidor",
        nullable=False,
        index=True
    )

    mes: int
    ano: int

    observacao: str = Field(sa_column=Column("observacao", Text, nullable=False))
    flag_teto: bool = Field(sa_column=Column("flag_teto", Boolean, nullable=False))

    servidor: Optional["Servidor"] = Relationship(back_populates="observacoes")