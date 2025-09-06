from typing import Optional
from sqlmodel import SQLModel


class RemuneracaoBase(SQLModel):
    id_servidor: int
    mes: int
    ano: int
    remuneracao: float
    irrf: float
    pss_rpgs: float
    remuneracao_final: float


class RemuneracaoRead(RemuneracaoBase):
    id_remuneracao: int


class RemuneracaoCreate(RemuneracaoBase):
    pass