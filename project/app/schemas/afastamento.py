from typing import Optional
from datetime import date
from sqlmodel import SQLModel


class AfastamentoBase(SQLModel):
    id_servidor: int
    mes: int
    ano: int
    inicio_afastamento: Optional[date]
    duracao_dias: int = 1


class AfastamentoRead(AfastamentoBase):
    id_afastamento: int


class AfastamentoCreate(AfastamentoBase):
    pass