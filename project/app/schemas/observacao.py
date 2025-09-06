from sqlmodel import SQLModel
from typing import Optional


class ObservacaoBase(SQLModel):
    id_servidor: int
    mes: int
    ano: int
    observacao: str
    flag_teto: bool


class ObservacaoRead(ObservacaoBase):
    id_observacao: int


class ObservacaoCreate(ObservacaoBase):
    pass