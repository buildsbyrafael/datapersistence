from sqlmodel import SQLModel
from app.core.database import engine

from app.models.servidor import Servidor
from app.models.remuneracao import Remuneracao
from app.models.afastamento import Afastamento
from app.models.observacao import Observacao
from app.models.cargofuncao import CargoFuncao
from app.models.funcaocargo import FuncaoCargo

def init_db():
    SQLModel.metadata.create_all(engine)

if __name__ == "__main__":
    init_db()
    print("Banco de dados inicializado com sucesso!")