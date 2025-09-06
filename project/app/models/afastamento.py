from typing import Optional, TYPE_CHECKING
from datetime import date
from sqlmodel import SQLModel, Field, Relationship
from sqlalchemy import Column, Integer, Date, ForeignKey
from typing import List

from app.schemas.servidor import ServidorRead

if TYPE_CHECKING:
    from app.models.servidor import Servidor


class AfastamentoBase(SQLModel):
    id_servidor: int = Field(
        foreign_key="servidores.id_servidor",
        nullable=False,
        index=True,
        description="ID do servidor que teve o afastamento"
    )
    
    mes: int = Field(
        ge=1,
        le=12,
        description="Mês do afastamento (1-12)"
    )
    
    ano: int = Field(
        ge=1900,
        le=2100,
        description="Ano do afastamento"
    )
    
    inicio_afastamento: Optional[date] = Field(
        default=None,
        sa_column=Column("inicio_afastamento", Date, nullable=True),
        description="Data de início do afastamento"
    )
    
    duracao_dias: int = Field(
        default=1,
        ge=1,
        sa_column=Column("duracao_dias", Integer, nullable=False),
        description="Duração do afastamento em dias"
    )


class Afastamento(AfastamentoBase, table=True):
    """Modelo principal de Afastamento para banco de dados"""
    __tablename__ = "afastamentos"

    id_afastamento: Optional[int] = Field(
        default=None,
        sa_column=Column("id_afastamento", Integer, primary_key=True, autoincrement=True),
        description="ID único do afastamento"
    )

    # Relacionamentos
    servidor: Optional["Servidor"] = Relationship(
        back_populates="afastamentos",
        sa_relationship_kwargs={"lazy": "select"}
    )

    class Config:
        """Configurações do modelo"""
        from_attributes = True
        json_encoders = {
            date: lambda v: v.isoformat() if v else None
        }

    def __repr__(self) -> str:
        return f"<Afastamento(id={self.id_afastamento}, servidor_id={self.id_servidor}, duracao={self.duracao_dias} dias)>"

    def __str__(self) -> str:
        return f"Afastamento {self.id_afastamento} - Servidor {self.id_servidor} ({self.mes}/{self.ano})"

    @property
    def data_fim_calculada(self) -> Optional[date]:
        """Calcula a data de fim do afastamento baseada no início e duração"""
        if self.inicio_afastamento and self.duracao_dias:
            from datetime import timedelta
            return self.inicio_afastamento + timedelta(days=self.duracao_dias - 1)
        return None

    @property
    def periodo_formatado(self) -> str:
        """Retorna o período formatado como string"""
        return f"{self.mes:02d}/{self.ano}"

    def is_afastamento_longo(self, limite_dias: int = 30) -> bool:
        """Verifica se é um afastamento considerado longo"""
        return self.duracao_dias > limite_dias

    def is_mesmo_periodo(self, outro_ano: int, outro_mes: int) -> bool:
        """Verifica se o afastamento é do mesmo período (ano/mês)"""
        return self.ano == outro_ano and self.mes == outro_mes


class AfastamentoCreate(AfastamentoBase):
    """Schema para criação de afastamento"""
    pass


class AfastamentoUpdate(SQLModel):
    """Schema para atualização parcial de afastamento"""
    id_servidor: Optional[int] = Field(
        default=None,
        foreign_key="servidores.id_servidor",
        description="ID do servidor que teve o afastamento"
    )
    
    mes: Optional[int] = Field(
        default=None,
        ge=1,
        le=12,
        description="Mês do afastamento (1-12)"
    )
    
    ano: Optional[int] = Field(
        default=None,
        ge=1900,
        le=2100,
        description="Ano do afastamento"
    )
    
    inicio_afastamento: Optional[date] = Field(
        default=None,
        description="Data de início do afastamento"
    )
    
    duracao_dias: Optional[int] = Field(
        default=None,
        ge=1,
        description="Duração do afastamento em dias"
    )


class AfastamentoRead(AfastamentoBase):
    """Schema para leitura de afastamento"""
    id_afastamento: int = Field(description="ID único do afastamento")
    
    # Campos calculados opcionais
    data_fim_calculada: Optional[date] = Field(
        default=None,
        description="Data de fim calculada baseada no início e duração"
    )
    
    periodo_formatado: Optional[str] = Field(
        default=None,
        description="Período formatado como MM/YYYY"
    )
    
    class Config:
        from_attributes = True


class AfastamentoReadWithServidor(AfastamentoRead):
    """Schema para leitura de afastamento incluindo dados do servidor"""
    servidor: Optional["ServidorRead"] = Field(
        default=None,
        description="Dados do servidor relacionado"
    )


# Schema para respostas de estatísticas
class AfastamentoEstatisticas(SQLModel):
    """Schema para estatísticas de afastamentos"""
    id_servidor: Optional[int] = Field(description="ID do servidor (opcional para estatísticas gerais)")
    ano: Optional[int] = Field(description="Ano das estatísticas (opcional)")
    total_afastamentos: int = Field(description="Total de afastamentos")
    total_dias: int = Field(description="Total de dias de afastamento")
    media_dias: float = Field(description="Média de dias por afastamento")
    maior_afastamento: int = Field(description="Maior afastamento em dias")
    menor_afastamento: int = Field(description="Menor afastamento em dias")


# Schema para resposta de importação
class AfastamentoImportResponse(SQLModel):
    """Schema para resposta de importação de afastamentos"""
    mensagem: str = Field(description="Mensagem de sucesso")
    total_importados: int = Field(description="Total de registros importados")
    erros: Optional[List[str]] = Field(default=None, description="Lista de erros encontrados")


# Schema para contagem
class AfastamentoContagem(SQLModel):
    """Schema para resposta de contagem de afastamentos"""
    ano: Optional[int] = Field(default=None, description="Ano filtrado")
    mes: Optional[int] = Field(default=None, description="Mês filtrado")
    id_servidor: Optional[int] = Field(default=None, description="ID do servidor filtrado")
    quantidade: int = Field(description="Quantidade de afastamentos encontrados")


# Importação tardia para evitar problemas de circular import
