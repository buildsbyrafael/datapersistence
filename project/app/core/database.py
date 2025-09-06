from sqlmodel import SQLModel, create_engine, Session
from app.core.config import settings

from app.models.servidor import Servidor
from app.models.remuneracao import Remuneracao
from app.models.afastamento import Afastamento
from app.models.observacao import Observacao
from app.models.cargofuncao import CargoFuncao
from app.models.funcaocargo import FuncaoCargo

engine = create_engine(settings.DATABASE_URL, echo=True)

def get_session():
    with Session(engine) as session:
        yield session

def init_db():
    print("-----------------")
    print(settings.DATABASE_URL)
    print("-----------------")
    SQLModel.metadata.create_all(engine)