from typing import TYPE_CHECKING, Optional
from datetime import date
from sqlmodel import SQLModel, Field, Relationship
from sqlalchemy import Column, Integer, Date, ForeignKey

if TYPE_CHECKING:
    from app.models.servidor import Servidor
    from app.models.cargofuncao import CargoFuncao


class FuncaoCargo(SQLModel, table=True):
    __tablename__ = "funcao_cargo"

    id_servidor_funcao: Optional[int] = Field(
        default=None,
        sa_column=Column("id_servidor_funcao", Integer, primary_key=True, autoincrement=True)
    )

    id_servidor: int = Field(
        foreign_key="servidores.id_servidor",
        nullable=False,
        index=True
    )

    id_cargo_funcao: int = Field(
        foreign_key="cargofuncao.id_cargo_funcao",
        nullable=False,
        index=True
    )

    data_ingresso_funcao: Optional[date] = Field(
        default=None,
        sa_column=Column("data_ingresso_funcao", Date, nullable=True)
    )

    servidor: Optional["Servidor"] = Relationship(back_populates="funcoes_cargos")
    cargo_funcao: Optional["CargoFuncao"] = Relationship()