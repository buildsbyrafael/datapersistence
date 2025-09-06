from typing import TYPE_CHECKING, Optional, List
from sqlmodel import SQLModel, Field, Relationship
from sqlalchemy import Column, String, BigInteger

if TYPE_CHECKING:
    from app.models.funcaocargo import FuncaoCargo


class CargoFuncao(SQLModel, table=True):
    __tablename__ = "cargofuncao"

    id_cargo_funcao: Optional[int] = Field(
        default=None,
        sa_column=Column("id_cargo_funcao", BigInteger, primary_key=True, autoincrement=True)
    )

    classe_cargo: Optional[str] = Field(
        default=None,
        sa_column=Column("classe_cargo", String, nullable=True)
    )

    referencia_cargo: Optional[int] = Field(
        default=None,
        sa_column=Column("referencia_cargo", BigInteger, nullable=True)
    )

    padrao_cargo: Optional[int] = Field(
        default=None,
        sa_column=Column("padrao_cargo", BigInteger, nullable=True)
    )

    nivel_cargo: Optional[int] = Field(
        default=None,
        sa_column=Column("nivel_cargo", BigInteger, nullable=True)
    )

    funcao: Optional[str] = Field(
        default=None,
        sa_column=Column("funcao", String, nullable=True)
    )

    descricao_cargo: str = Field(
        sa_column=Column("descricao_cargo", String, nullable=False)
    )

    nivel_funcao: Optional[int] = Field(
        default=None,
        sa_column=Column("nivel_funcao", BigInteger, nullable=True)
    )
    
    funcoes_cargos: List["FuncaoCargo"] = Relationship(back_populates="cargo_funcao")