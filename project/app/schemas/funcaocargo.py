from typing import Optional
from datetime import date
from sqlmodel import SQLModel


class FuncaoCargoBase(SQLModel):
    id_servidor: int
    id_cargo_funcao: int
    data_ingresso_funcao: Optional[date] = None


class FuncaoCargoRead(FuncaoCargoBase):
    id_servidor_funcao: int


class FuncaoCargoCreate(FuncaoCargoBase):
    pass

class FuncaoCargoUpdate(SQLModel):
    id_servidor: Optional[int] = None
    id_cargo_funcao: Optional[int] = None
    data_ingresso_funcao: Optional[date] = None