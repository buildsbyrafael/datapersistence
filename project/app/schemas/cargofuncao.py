from typing import Optional
from sqlmodel import SQLModel


class CargoFuncaoBase(SQLModel):
    classe_cargo: Optional[str]
    referencia_cargo: Optional[int]
    padrao_cargo: Optional[int]
    nivel_cargo: Optional[int]
    funcao: Optional[str]
    descricao_cargo: str
    nivel_funcao: Optional[int]


class CargoFuncaoRead(CargoFuncaoBase):
    id_cargo_funcao: int


class CargoFuncaoCreate(CargoFuncaoBase):
    pass