from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field
from datetime import datetime

class RelatorioRequest(BaseModel):
    """Schema para request do relatório"""
    ano: int = Field(..., description="Ano para análise", ge=2020, le=2030)
    mes_inicio: Optional[int] = Field(None, description="Mês de início (1-12)", ge=1, le=12)
    mes_fim: Optional[int] = Field(None, description="Mês de fim (1-12)", ge=1, le=12)
    incluir_graficos: bool = Field(True, description="Se deve gerar gráficos")
    formato_saida: str = Field("json", description="Formato de saída: json, excel, pdf")

class InsightResponse(BaseModel):
    """Schema para resposta de insight"""
    tipo: str
    titulo: str
    valor: Any  # Permite qualquer tipo de valor
    descricao: str
    periodo: Optional[str] = None
    
    class Config:
        # Permite conversão automática de tipos
        json_encoders = {
            # Converte qualquer valor para string na serialização JSON se necessário
            int: str,
            float: lambda v: f"{v:.2f}",
        }

class ResumoGeralResponse(BaseModel):
    """Schema para resumo geral"""
    total_servidores: int
    servidores_ativos: int
    total_remuneracao: float
    media_remuneracao: float
    taxa_atividade: float

class TopRemuneracaoResponse(BaseModel):
    """Schema para top remunerações"""
    nome: str
    cargo: str
    media_anual: float

class RelatorioCompletoResponse(BaseModel):
    """Schema para relatório completo"""
    periodo: str
    data_geracao: datetime
    resumo_geral: ResumoGeralResponse
    insights: List[InsightResponse]
    graficos_gerados: List[str]
    top_remuneracoes: List[TopRemuneracaoResponse]

class StatusResponse(BaseModel):
    """Schema para status das operações"""
    sucesso: bool
    mensagem: str
    dados: Optional[Dict[str, Any]] = None
