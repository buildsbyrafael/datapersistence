from logging.handlers import RotatingFileHandler
import os
from fastapi import FastAPI, Request
from sqlmodel import select

from app.core.database import get_session, init_db
from app.models.servidor import Servidor
from app.models.remuneracao import Remuneracao
from app.models.afastamento import Afastamento
from app.models.observacao import Observacao
from app.models.cargofuncao import CargoFuncao
from app.models.funcaocargo import FuncaoCargo

from app.api.servidor import router as servidores_router
from app.api.remuneracao import router as remuneracoes_router
from app.api.afastamento import router as afastamentos_router
from app.api.observacao import router as observacoes_router
from app.api.cargofuncao import router as cargofuncao_router
from app.api.funcaocargo import router as funcoescargo_router
from app.api.analytics import router as analytics_router

import logging

# 1. Garante que a pasta exista
os.makedirs("logs", exist_ok=True)

# 2. Configura o logger
logger = logging.getLogger("myapp")
logger.setLevel(logging.INFO)

# 3. Handler de arquivo rotativo (máx 5 MB, 3 backups)
file_handler = RotatingFileHandler(
    "logs/app.log",
    maxBytes=5*1024*1024,
    backupCount=3,
    encoding="utf-8"
)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter(
    "%(asctime)s — %(name)s — %(levelname)s — %(message)s"
))
logger.addHandler(file_handler)

# 4. (Opcional) Também manda logs para o console
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter(
    "%(asctime)s — %(levelname)s — %(message)s"
))
logger.addHandler(console_handler)

app = FastAPI(
    title="Portal Transparência API",
    version="0.1.0"
)

app.include_router(servidores_router)
app.include_router(remuneracoes_router)
app.include_router(afastamentos_router)
app.include_router(observacoes_router)
app.include_router(cargofuncao_router)
app.include_router(funcoescargo_router)
app.include_router(analytics_router)

@app.middleware("http")
async def log_requests(request: Request, call_next):
    logger.info(f"→ {request.method} {request.url.path}")
    try:
        response = await call_next(request)
    except Exception as e:
        logger.exception("Erro ao processar requisição")
        raise
    logger.info(f"← {request.method} {request.url.path} — status {response.status_code}")
    return response

@app.on_event("startup")
def on_startup():
    init_db()

    for session in get_session():
        try:
            servidores = session.exec(select(Servidor).limit(5)).all()
            print(f"[STARTUP] Servidores no banco! {len(servidores)}!")
            for r in servidores:
                print(f"  - {r.id_servidor}: {r.nome}")
        except Exception as e:
            print(f"[ERRO] Não foi possível consultar servidores! {e}!")

        try:
            remuneracoes = session.exec(select(Remuneracao).limit(5)).all()
            print(f"[STARTUP] Remunerações no banco! {len(remuneracoes)}!")
            for r in remuneracoes:
                print(f"  - Servidor {r.id_servidor} | {r.ano}-{r.mes} → R$ {r.remuneracao_final:.2f}")
        except Exception as e:
            print(f"[ERRO] Não foi possível consultar remunerações! {e}!")

        try:
            afastamentos = session.exec(select(Afastamento).limit(5)).all()
            print(f"[STARTUP] Afastamentos no banco! {len(afastamentos)}!")
            for r in afastamentos:
                data_fmt = r.inicio_afastamento.strftime('%d/%m/%Y') if r.inicio_afastamento else "SEM DATA"
                print(f"  - Servidor {r.id_servidor} | {r.ano}-{r.mes} → Início: {data_fmt}")
        except Exception as e:
            print(f"[ERRO] Não foi possível consultar afastamentos! {e}!")

        try:
            observacoes = session.exec(select(Observacao).limit(5)).all()
            print(f"[STARTUP] Observações no banco! {len(observacoes)}!")
            for r in observacoes:
                status = "ACIMA DO TETO" if r.flag_teto else "OK"
                print(f"  - Servidor {r.id_servidor} | {r.ano}-{r.mes} → {status}")
        except Exception as e:
            print(f"[ERRO] Não foi possível consultar observações! {e}!")

        try:
            cargosfuncoes = session.exec(select(CargoFuncao).limit(5)).all()
            print(f"[STARTUP] Cargos/Funções no banco! {len(cargosfuncoes)}!")
            for r in cargosfuncoes:
                resumo = f"{r.classe_cargo or '_'}-{r.referencia_cargo or '_'}-{r.padrao_cargo or '_'}"
                print(f"  - {resumo} → {r.descricao_cargo}")
        except Exception as e:
            print(f"[ERRO] Não foi possível consultar cargos/funções! {e}!")

        try:
            funcoescargos = session.exec(select(FuncaoCargo).limit(5)).all()
            print(f"[STARTUP] Vínculos Função/Cargo no banco! {len(funcoescargos)}!")
            for r in funcoescargos:
                data_fmt = r.data_ingresso_funcao.strftime('%d/%m/%Y') if r.data_ingresso_funcao else "SEM DATA"
                print(f"  - Servidor {r.id_servidor} ↔ CargoFuncao {r.id_cargo_funcao} em {data_fmt}")
        except Exception as e:
            print(f"[ERRO] Não foi possível consultar vínculos função/cargo! {e}!")