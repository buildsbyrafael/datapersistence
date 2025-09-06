from typing import Optional
from sqlmodel import SQLModel

class ServidorBase(SQLModel):
    nome: str
    cpf: str
    descr_cargo: str
    org_superior: str
    org_exercicio: str
    regime: str
    jornada_trabalho: str

class ServidorRead(ServidorBase):
    id_servidor: int

class ServidorCreate(ServidorBase):
    pass