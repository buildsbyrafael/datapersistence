"""
Módulo de Análise de Dados - Servidores Públicos
Processa dados e gera insights sobre remuneração, afastamentos e distribuição de servidores
"""

from typing import Dict, List, Tuple, Optional, Any
from datetime import datetime, date
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy.orm import Session
from sqlalchemy import func, extract, desc, and_
import numpy as np
from dataclasses import dataclass

from app.models.afastamento import Afastamento
from app.models.remuneracao import Remuneracao
from app.models.servidor import Servidor



@dataclass
class InsightSummary:
    """Classe para estruturar insights gerados"""
    tipo: str
    titulo: str
    valor: Any
    descricao: str
    periodo: Optional[str] = None


class ServidorAnalytics:
    """Classe principal para análise de dados dos servidores"""
    
    def __init__(self, session: Session):
        self.session = session
        self.insights: List[InsightSummary] = []
    
    def gerar_relatorio_completo(self, ano: int = None) -> Dict[str, Any]:
        """Gera relatório completo com todos os insights"""
        if ano is None:
            ano = datetime.now().year
            
        relatorio = {
            'periodo': f"Ano {ano}",
            'resumo_geral': self._resumo_geral(ano),
            'analise_remuneracao': self._analise_remuneracao(ano),
            'analise_afastamentos': self._analise_afastamentos(ano),
            'distribuicao_organizacional': self._distribuicao_organizacional(),
            'insights': self.insights,
            'graficos_gerados': []
        }
        
        # Gerar gráficos
        relatorio['graficos_gerados'] = self._gerar_graficos(ano)
        
        return relatorio
    
    def _resumo_geral(self, ano: int) -> Dict[str, Any]:
        """Gera resumo geral dos servidores"""
        total_servidores = self.session.query(Servidor).count()
        
        # Servidores com remuneração no ano
        servidores_ativos = self.session.query(Servidor.id_servidor)\
            .join(Remuneracao)\
            .filter(Remuneracao.ano == ano)\
            .distinct().count()
        
        # Total de remuneração paga no ano
        total_remuneracao = self.session.query(func.sum(Remuneracao.remuneracao_final))\
            .filter(Remuneracao.ano == ano).scalar() or 0
        
        # Média de remuneração
        media_remuneracao = self.session.query(func.avg(Remuneracao.remuneracao_final))\
            .filter(Remuneracao.ano == ano).scalar() or 0
        
        self.insights.append(InsightSummary(
            tipo="geral",
            titulo="Total de Servidores Ativos",
            valor=servidores_ativos,
            descricao=f"De {total_servidores} servidores cadastrados, {servidores_ativos} estiveram ativos em {ano}",
            periodo=str(ano)
        ))
        
        return {
            'total_servidores': total_servidores,
            'servidores_ativos': servidores_ativos,
            'total_remuneracao': round(total_remuneracao, 2),
            'media_remuneracao': round(media_remuneracao, 2),
            'taxa_atividade': round((servidores_ativos / total_servidores) * 100, 2) if total_servidores > 0 else 0
        }
    
    def _analise_remuneracao(self, ano: int) -> Dict[str, Any]:
        """Análise detalhada da remuneração"""
        
        # Estatísticas básicas de remuneração
        stats = self.session.query(
            func.min(Remuneracao.remuneracao_final).label('minima'),
            func.max(Remuneracao.remuneracao_final).label('maxima'),
            func.avg(Remuneracao.remuneracao_final).label('media'),
            func.count(Remuneracao.id_remuneracao).label('total_registros')
        ).filter(Remuneracao.ano == ano).first()
        
        # Top 10 maiores remunerações
        top_remuneracoes = self.session.query(
            Servidor.nome,
            Servidor.descr_cargo,
            func.avg(Remuneracao.remuneracao_final).label('media_anual')
        ).join(Remuneracao)\
         .filter(Remuneracao.ano == ano)\
         .group_by(Servidor.id_servidor, Servidor.nome, Servidor.descr_cargo)\
         .order_by(desc('media_anual'))\
         .limit(10).all()
        
        # Análise por cargo
        remuneracao_por_cargo = self.session.query(
            Servidor.descr_cargo,
            func.count(Servidor.id_servidor).label('quantidade'),
            func.avg(Remuneracao.remuneracao_final).label('media_remuneracao')
        ).join(Remuneracao)\
         .filter(Remuneracao.ano == ano)\
         .group_by(Servidor.descr_cargo)\
         .order_by(desc('media_remuneracao')).all()
        
        # Identificar disparidades salariais
        if stats.maxima and stats.minima:
            disparidade = stats.maxima / stats.minima if stats.minima > 0 else 0
            self.insights.append(InsightSummary(
                tipo="remuneracao",
                titulo="Disparidade Salarial",
                valor=f"{disparidade:.1f}x",
                descricao=f"A maior remuneração é {disparidade:.1f} vezes maior que a menor",
                periodo=str(ano)
            ))
        
        return {
            'estatisticas': {
                'minima': round(stats.minima or 0, 2),
                'maxima': round(stats.maxima or 0, 2),
                'media': round(stats.media or 0, 2),
                'total_registros': stats.total_registros or 0
            },
            'top_remuneracoes': [
                {
                    'nome': r.nome,
                    'cargo': r.descr_cargo,
                    'media_anual': round(r.media_anual, 2)
                } for r in top_remuneracoes
            ],
            'remuneracao_por_cargo': [
                {
                    'cargo': r.descr_cargo,
                    'quantidade': r.quantidade,
                    'media_remuneracao': round(r.media_remuneracao, 2)
                } for r in remuneracao_por_cargo
            ]
        }
    
    def _analise_afastamentos(self, ano: int) -> Dict[str, Any]:
        """Análise de afastamentos dos servidores"""
        
        # Estatísticas gerais de afastamentos
        total_afastamentos = self.session.query(Afastamento)\
            .filter(Afastamento.ano == ano).count()
        
        total_dias_afastamento = self.session.query(func.sum(Afastamento.duracao_dias))\
            .filter(Afastamento.ano == ano).scalar() or 0
        
        # Servidores com mais afastamentos
        servidores_afastamentos = self.session.query(
            Servidor.nome,
            Servidor.descr_cargo,
            func.count(Afastamento.id_afastamento).label('total_afastamentos'),
            func.sum(Afastamento.duracao_dias).label('total_dias')
        ).join(Afastamento)\
         .filter(Afastamento.ano == ano)\
         .group_by(Servidor.id_servidor, Servidor.nome, Servidor.descr_cargo)\
         .order_by(desc('total_dias'))\
         .limit(10).all()
        
        # Afastamentos por mês
        afastamentos_por_mes = self.session.query(
            Afastamento.mes,
            func.count(Afastamento.id_afastamento).label('quantidade'),
            func.sum(Afastamento.duracao_dias).label('total_dias')
        ).filter(Afastamento.ano == ano)\
         .group_by(Afastamento.mes)\
         .order_by(Afastamento.mes).all()
        
        # Calcular taxa de afastamento
        servidores_ativos = self.session.query(Servidor.id_servidor)\
            .join(Remuneracao)\
            .filter(Remuneracao.ano == ano)\
            .distinct().count()
        
        taxa_afastamento = (total_afastamentos / servidores_ativos) * 100 if servidores_ativos > 0 else 0
        
        self.insights.append(InsightSummary(
            tipo="afastamento",
            titulo="Taxa de Afastamento",
            valor=f"{taxa_afastamento:.1f}%",
            descricao=f"Taxa de afastamentos em relação aos servidores ativos",
            periodo=str(ano)
        ))
        
        return {
            'total_afastamentos': total_afastamentos,
            'total_dias_afastamento': total_dias_afastamento,
            'taxa_afastamento': round(taxa_afastamento, 2),
            'servidores_mais_afastados': [
                {
                    'nome': s.nome,
                    'cargo': s.descr_cargo,
                    'afastamentos': s.total_afastamentos,
                    'dias_total': s.total_dias
                } for s in servidores_afastamentos
            ],
            'afastamentos_por_mes': [
                {
                    'mes': a.mes,
                    'quantidade': a.quantidade,
                    'total_dias': a.total_dias
                } for a in afastamentos_por_mes
            ]
        }
    
    def _distribuicao_organizacional(self) -> Dict[str, Any]:
        """Análise da distribuição organizacional"""
        
        # Distribuição por órgão superior
        dist_org_superior = self.session.query(
            Servidor.org_superior,
            func.count(Servidor.id_servidor).label('quantidade')
        ).group_by(Servidor.org_superior)\
         .order_by(desc('quantidade')).all()
        
        # Distribuição por órgão de exercício
        dist_org_exercicio = self.session.query(
            Servidor.org_exercicio,
            func.count(Servidor.id_servidor).label('quantidade')
        ).group_by(Servidor.org_exercicio)\
         .order_by(desc('quantidade')).limit(15).all()
        
        # Distribuição por regime
        dist_regime = self.session.query(
            Servidor.regime,
            func.count(Servidor.id_servidor).label('quantidade')
        ).group_by(Servidor.regime)\
         .order_by(desc('quantidade')).all()
        
        # Distribuição por jornada
        dist_jornada = self.session.query(
            Servidor.jornada_trabalho,
            func.count(Servidor.id_servidor).label('quantidade')
        ).group_by(Servidor.jornada_trabalho)\
         .order_by(desc('quantidade')).all()
        
        return {
            'por_org_superior': [
                {'orgao': d.org_superior, 'quantidade': d.quantidade}
                for d in dist_org_superior
            ],
            'por_org_exercicio': [
                {'orgao': d.org_exercicio, 'quantidade': d.quantidade}
                for d in dist_org_exercicio
            ],
            'por_regime': [
                {'regime': d.regime, 'quantidade': d.quantidade}
                for d in dist_regime
            ],
            'por_jornada': [
                {'jornada': d.jornada_trabalho, 'quantidade': d.quantidade}
                for d in dist_jornada
            ]
        }
    
    def _gerar_graficos(self, ano: int) -> List[str]:
        """Gera gráficos para visualização dos dados"""
        graficos_gerados = []
        
        # Configurar estilo dos gráficos
        plt.style.use('default')
        sns.set_palette("husl")
        
        try:
            # Gráfico 1: Evolução mensal da remuneração
            self._grafico_evolucao_remuneracao(ano)
            graficos_gerados.append("evolucao_remuneracao_mensal.png")
            
            # Gráfico 2: Distribuição de remuneração por cargo (top 10)
            self._grafico_remuneracao_por_cargo(ano)
            graficos_gerados.append("remuneracao_por_cargo.png")
            
            # Gráfico 3: Afastamentos por mês
            self._grafico_afastamentos_mensais(ano)
            graficos_gerados.append("afastamentos_por_mes.png")
            
            # Gráfico 4: Distribuição por órgão superior
            self._grafico_distribuicao_organizacional()
            graficos_gerados.append("distribuicao_organizacional.png")
            
            # Gráfico 5: Análise de dispersão - Remuneração vs Afastamentos
            self._grafico_remuneracao_vs_afastamentos(ano)
            graficos_gerados.append("remuneracao_vs_afastamentos.png")
            
        except Exception as e:
            print(f"Erro ao gerar gráficos: {e}")
        
        return graficos_gerados
    
    def _grafico_evolucao_remuneracao(self, ano: int):
        """Gráfico da evolução mensal da remuneração média"""
        dados = self.session.query(
            Remuneracao.mes,
            func.avg(Remuneracao.remuneracao_final).label('media'),
            func.count(Remuneracao.id_remuneracao).label('quantidade')
        ).filter(Remuneracao.ano == ano)\
         .group_by(Remuneracao.mes)\
         .order_by(Remuneracao.mes).all()
        
        if not dados:
            return
        
        meses = [d.mes for d in dados]
        medias = [float(d.media) for d in dados]
        
        plt.figure(figsize=(12, 6))
        plt.plot(meses, medias, marker='o', linewidth=2, markersize=8)
        plt.title(f'Evolução da Remuneração Média Mensal - {ano}', fontsize=14, fontweight='bold')
        plt.xlabel('Mês')
        plt.ylabel('Remuneração Média (R$)')
        plt.grid(True, alpha=0.3)
        plt.xticks(range(1, 13))
        
        # Formatação dos valores no eixo Y
        ax = plt.gca()
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'R$ {x:,.0f}'))
        
        plt.tight_layout()
        plt.savefig('evolucao_remuneracao_mensal.png', dpi=300, bbox_inches='tight')
        plt.close()
    
    def _grafico_remuneracao_por_cargo(self, ano: int):
        """Gráfico de remuneração média por cargo (top 10)"""
        dados = self.session.query(
            Servidor.descr_cargo,
            func.avg(Remuneracao.remuneracao_final).label('media'),
            func.count(Servidor.id_servidor).label('quantidade')
        ).join(Remuneracao)\
         .filter(Remuneracao.ano == ano)\
         .group_by(Servidor.descr_cargo)\
         .having(func.count(Servidor.id_servidor) >= 5)\
         .order_by(desc('media'))\
         .limit(10).all()
        
        if not dados:
            return
        
        cargos = [d.descr_cargo[:30] + '...' if len(d.descr_cargo) > 30 else d.descr_cargo for d in dados]
        medias = [float(d.media) for d in dados]
        
        plt.figure(figsize=(12, 8))
        bars = plt.barh(cargos, medias)
        plt.title('Top 10 Cargos - Remuneração Média', fontsize=14, fontweight='bold')
        plt.xlabel('Remuneração Média (R$)')
        
        # Colorir as barras
        for i, bar in enumerate(bars):
            bar.set_color(plt.cm.viridis(i / len(bars)))
        
        # Adicionar valores nas barras
        for i, v in enumerate(medias):
            plt.text(v + max(medias) * 0.01, i, f'R$ {v:,.0f}', va='center')
        
        plt.gca().xaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'R$ {x:,.0f}'))
        plt.tight_layout()
        plt.savefig('remuneracao_por_cargo.png', dpi=300, bbox_inches='tight')
        plt.close()
    
    def _grafico_afastamentos_mensais(self, ano: int):
        """Gráfico de afastamentos por mês"""
        dados = self.session.query(
            Afastamento.mes,
            func.count(Afastamento.id_afastamento).label('quantidade'),
            func.sum(Afastamento.duracao_dias).label('total_dias')
        ).filter(Afastamento.ano == ano)\
         .group_by(Afastamento.mes)\
         .order_by(Afastamento.mes).all()
        
        if not dados:
            return
        
        meses = [d.mes for d in dados]
        quantidades = [d.quantidade for d in dados]
        total_dias = [d.total_dias for d in dados]
        
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))
        
        # Gráfico 1: Quantidade de afastamentos
        ax1.bar(meses, quantidades, color='skyblue', alpha=0.7)
        ax1.set_title(f'Quantidade de Afastamentos por Mês - {ano}', fontweight='bold')
        ax1.set_ylabel('Quantidade de Afastamentos')
        ax1.set_xticks(range(1, 13))
        ax1.grid(True, alpha=0.3)
        
        # Gráfico 2: Total de dias de afastamento
        ax2.bar(meses, total_dias, color='coral', alpha=0.7)
        ax2.set_title(f'Total de Dias de Afastamento por Mês - {ano}', fontweight='bold')
        ax2.set_xlabel('Mês')
        ax2.set_ylabel('Total de Dias')
        ax2.set_xticks(range(1, 13))
        ax2.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig('afastamentos_por_mes.png', dpi=300, bbox_inches='tight')
        plt.close()
    
    def _grafico_distribuicao_organizacional(self):
        """Gráfico de distribuição por órgão superior"""
        dados = self.session.query(
            Servidor.org_superior,
            func.count(Servidor.id_servidor).label('quantidade')
        ).group_by(Servidor.org_superior)\
         .order_by(desc('quantidade')).limit(10).all()
        
        if not dados:
            return
        
        orgaos = [d.org_superior[:25] + '...' if len(d.org_superior) > 25 else d.org_superior for d in dados]
        quantidades = [d.quantidade for d in dados]
        
        plt.figure(figsize=(10, 8))
        colors = plt.cm.Set3(np.linspace(0, 1, len(orgaos)))
        wedges, texts, autotexts = plt.pie(quantidades, labels=orgaos, autopct='%1.1f%%', 
                                          colors=colors, startangle=90)
        
        plt.title('Distribuição de Servidores por Órgão Superior', fontsize=14, fontweight='bold')
        
        # Melhorar a aparência dos textos
        for autotext in autotexts:
            autotext.set_color('white')
            autotext.set_fontweight('bold')
        
        plt.axis('equal')
        plt.tight_layout()
        plt.savefig('distribuicao_organizacional.png', dpi=300, bbox_inches='tight')
        plt.close()
    
    def _grafico_remuneracao_vs_afastamentos(self, ano: int):
        """Gráfico de dispersão: Remuneração vs Afastamentos"""
        dados = self.session.query(
            Servidor.nome,
            func.avg(Remuneracao.remuneracao_final).label('media_remuneracao'),
            func.coalesce(func.sum(Afastamento.duracao_dias), 0).label('total_afastamentos')
        ).outerjoin(Remuneracao)\
         .outerjoin(Afastamento, and_(
             Servidor.id_servidor == Afastamento.id_servidor,
             Afastamento.ano == ano
         ))\
         .filter(Remuneracao.ano == ano)\
         .group_by(Servidor.id_servidor, Servidor.nome)\
         .having(func.avg(Remuneracao.remuneracao_final) > 0).all()
        
        if not dados or len(dados) < 10:
            return
        
        remuneracoes = [float(d.media_remuneracao) for d in dados]
        afastamentos = [int(d.total_afastamentos) for d in dados]
        
        plt.figure(figsize=(10, 6))
        plt.scatter(remuneracoes, afastamentos, alpha=0.6, s=50)
        
        plt.title('Relação entre Remuneração e Dias de Afastamento', fontsize=14, fontweight='bold')
        plt.xlabel('Remuneração Média (R$)')
        plt.ylabel('Total de Dias de Afastamento')
        plt.grid(True, alpha=0.3)
        
        # Formatação do eixo X
        plt.gca().xaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'R$ {x:,.0f}'))
        
        # Linha de tendência
        if len(remuneracoes) > 1:
            z = np.polyfit(remuneracoes, afastamentos, 1)
            p = np.poly1d(z)
            plt.plot(remuneracoes, p(remuneracoes), "r--", alpha=0.8, linewidth=2)
        
        plt.tight_layout()
        plt.savefig('remuneracao_vs_afastamentos.png', dpi=300, bbox_inches='tight')
        plt.close()
    
    def gerar_relatorio_texto(self, relatorio: Dict[str, Any]) -> str:
        """Gera relatório em formato texto"""
        texto = f"""
RELATÓRIO DE ANÁLISE - SERVIDORES PÚBLICOS
==========================================
Período: {relatorio['periodo']}
Data de Geração: {datetime.now().strftime('%d/%m/%Y %H:%M')}

RESUMO GERAL
------------
• Total de Servidores Cadastrados: {relatorio['resumo_geral']['total_servidores']:,}
• Servidores Ativos no Período: {relatorio['resumo_geral']['servidores_ativos']:,}
• Taxa de Atividade: {relatorio['resumo_geral']['taxa_atividade']:.1f}%
• Total Pago em Remunerações: R$ {relatorio['resumo_geral']['total_remuneracao']:,.2f}
• Remuneração Média: R$ {relatorio['resumo_geral']['media_remuneracao']:,.2f}

ANÁLISE DE REMUNERAÇÃO
---------------------
• Menor Remuneração: R$ {relatorio['analise_remuneracao']['estatisticas']['minima']:,.2f}
• Maior Remuneração: R$ {relatorio['analise_remuneracao']['estatisticas']['maxima']:,.2f}
• Remuneração Média: R$ {relatorio['analise_remuneracao']['estatisticas']['media']:,.2f}
• Total de Registros: {relatorio['analise_remuneracao']['estatisticas']['total_registros']:,}

Top 5 Maiores Remunerações:
"""
        
        for i, servidor in enumerate(relatorio['analise_remuneracao']['top_remuneracoes'][:5], 1):
            texto += f"{i}. {servidor['nome']} - {servidor['cargo']} - R$ {servidor['media_anual']:,.2f}\n"
        
        texto += f"""
ANÁLISE DE AFASTAMENTOS
----------------------
• Total de Afastamentos: {relatorio['analise_afastamentos']['total_afastamentos']:,}
• Total de Dias de Afastamento: {relatorio['analise_afastamentos']['total_dias_afastamento']:,}
• Taxa de Afastamento: {relatorio['analise_afastamentos']['taxa_afastamento']:.2f}%

PRINCIPAIS INSIGHTS
------------------
"""
        
        for insight in relatorio['insights']:
            texto += f"• {insight.titulo}: {insight.valor} - {insight.descricao}\n"
        
        texto += f"""
GRÁFICOS GERADOS
---------------
"""
        for grafico in relatorio['graficos_gerados']:
            texto += f"• {grafico}\n"
        
        return texto
    
    def exportar_dados_excel(self, relatorio: Dict[str, Any], nome_arquivo: str = None):
        """Exporta dados para Excel"""
        if nome_arquivo is None:
            nome_arquivo = f"relatorio_servidores_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        
        with pd.ExcelWriter(nome_arquivo, engine='openpyxl') as writer:
            # Aba 1: Resumo Geral
            resumo_df = pd.DataFrame([relatorio['resumo_geral']])
            resumo_df.to_excel(writer, sheet_name='Resumo Geral', index=False)
            
            # Aba 2: Top Remunerações
            top_rem_df = pd.DataFrame(relatorio['analise_remuneracao']['top_remuneracoes'])
            top_rem_df.to_excel(writer, sheet_name='Top Remunerações', index=False)
            
            # Aba 3: Remuneração por Cargo
            cargo_rem_df = pd.DataFrame(relatorio['analise_remuneracao']['remuneracao_por_cargo'])
            cargo_rem_df.to_excel(writer, sheet_name='Remuneração por Cargo', index=False)
            
            # Aba 4: Insights
            insights_df = pd.DataFrame([
                {
                    'Tipo': i.tipo,
                    'Título': i.titulo,
                    'Valor': str(i.valor),
                    'Descrição': i.descricao,
                    'Período': i.periodo
                } for i in relatorio['insights']
            ])
            insights_df.to_excel(writer, sheet_name='Insights', index=False)
        
        return nome_arquivo


# Exemplo de uso
def exemplo_uso():
    """Exemplo de como usar a classe ServidorAnalytics"""
    
    # Assumindo que você tem uma sessão SQLAlchemy configurada
    # session = get_database_session()
    
    # analytics = ServidorAnalytics(session)
    # relatorio = analytics.gerar_relatorio_completo(2024)
    
    # # Gerar relatório em texto
    # relatorio_texto = analytics.gerar_relatorio_texto(relatorio)
    # print(relatorio_texto)
    
    # # Exportar para Excel
    # arquivo_excel = analytics.exportar_dados_excel(relatorio)
    # print(f"Relatório exportado para: {arquivo_excel}")
    
    pass


if __name__ == "__main__":
    exemplo_uso()