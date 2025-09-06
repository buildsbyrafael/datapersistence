# app/api/analytics.py
from datetime import datetime
from fastapi import APIRouter, BackgroundTasks, Depends, Path, Query, HTTPException
from fastapi.responses import PlainTextResponse, FileResponse
from sqlalchemy.orm import Session
from app.core.database import get_session
from typing import List, Optional
import os

from app.models.insights import ServidorAnalytics
from app.schemas.analytics import InsightResponse, RelatorioCompletoResponse, RelatorioRequest, ResumoGeralResponse, StatusResponse, TopRemuneracaoResponse

router = APIRouter(prefix="/analytics", tags=["Analytics"])

@router.get("/health", response_model=StatusResponse)
async def health_check():
    """Verifica se o serviço de analytics está funcionando"""
    return StatusResponse(
        sucesso=True,
        mensagem="Serviço de analytics operacional",
        dados={"timestamp": datetime.now().isoformat()}
    )

@router.get("/resumo/{ano}", response_model=ResumoGeralResponse)
async def get_resumo_geral(
    ano: int = Path(..., description="Ano para análise", ge=2020, le=2030),
    db: Session = Depends(get_session)
):
    """Retorna resumo geral dos servidores para um ano específico"""
    try:
        analytics = ServidorAnalytics(db)
        resumo = analytics._resumo_geral(ano)
        return ResumoGeralResponse(**resumo)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao gerar resumo: {str(e)}")

@router.get("/insights/{ano}", response_model=List[InsightResponse])
async def get_insights(
    ano: int = Path(..., description="Ano para análise"),
    db: Session = Depends(get_session)
):
    """Retorna insights específicos do ano"""
    try:
        analytics = ServidorAnalytics(db)
        relatorio = analytics.gerar_relatorio_completo(ano)
        
        # Converter insights para o schema de resposta
        insights_response = [
            InsightResponse(
                tipo=insight.tipo,
                titulo=insight.titulo,
                valor=str(insight.valor),  # Garantir conversão para string
                descricao=insight.descricao,
                periodo=insight.periodo
            ) for insight in analytics.insights
        ]
        
        return insights_response
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao gerar insights: {str(e)}")

@router.post("/relatorio-completo", response_model=RelatorioCompletoResponse)
async def gerar_relatorio_completo(
    request: RelatorioRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_session)
):
    """Gera relatório completo com todas as análises"""
    try:
        analytics = ServidorAnalytics(db)
        relatorio = analytics.gerar_relatorio_completo(request.ano)
        
        # Se solicitado, gerar arquivos em background
        if request.formato_saida == "excel":
            background_tasks.add_task(
                _gerar_excel_background, 
                analytics, 
                relatorio, 
                request.ano
            )
        
        # Converter insights para o schema de resposta
        insights_response = [
            InsightResponse(
                tipo=insight.tipo,
                titulo=insight.titulo,
                valor=str(insight.valor),  # Garantir conversão para string
                descricao=insight.descricao,
                periodo=insight.periodo
            ) for insight in analytics.insights
        ]
        
        top_remuneracoes = [
            TopRemuneracaoResponse(**item) 
            for item in relatorio['analise_remuneracao']['top_remuneracoes'][:5]
        ]
        
        return RelatorioCompletoResponse(
            periodo=relatorio['periodo'],
            data_geracao=datetime.now(),
            resumo_geral=ResumoGeralResponse(**relatorio['resumo_geral']),
            insights=insights_response,
            graficos_gerados=relatorio['graficos_gerados'],
            top_remuneracoes=top_remuneracoes
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao gerar relatório: {str(e)}")

@router.get("/graficos/{ano}")
async def gerar_graficos(
    ano: int,
    tipo_grafico: Optional[str] = Query(None, description="Tipo específico de gráfico"),
    db: Session = Depends(get_session)
):
    """Gera gráficos para visualização"""
    try:
        analytics = ServidorAnalytics(db)
        
        if tipo_grafico:
            # Gerar gráfico específico
            grafico_path = _gerar_grafico_especifico(analytics, ano, tipo_grafico)
            if grafico_path and os.path.exists(f"static/graficos/{grafico_path}"):
                return FileResponse(
                    f"static/graficos/{grafico_path}", 
                    media_type="image/png",
                    filename=f"{tipo_grafico}_{ano}.png"
                )
        else:
            # Gerar todos os gráficos
            graficos = analytics._gerar_graficos(ano)
            
            # Retornar URLs completas para download
            base_url = "http://localhost:8000"  # Configure conforme seu domínio
            graficos_urls = [
                {
                    "nome": grafico,
                    "url": f"{base_url}/static/graficos/{grafico}",
                    "download_url": f"{base_url}/analytics/download-grafico/{grafico.replace('.png', '')}"
                }
                for grafico in graficos
            ]
            
            return {
                "graficos_gerados": graficos,
                "links_download": graficos_urls
            }
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao gerar gráficos: {str(e)}")

@router.get("/download-grafico/{nome_grafico}")
async def download_grafico(nome_grafico: str):
    """Download de gráfico específico"""
    arquivo_path = f"static/graficos/{nome_grafico}.png"
    
    if not os.path.exists(arquivo_path):
        raise HTTPException(status_code=404, detail="Gráfico não encontrado")
    
    return FileResponse(
        arquivo_path,
        media_type="image/png",
        filename=f"{nome_grafico}.png",
        headers={"Content-Disposition": f"attachment; filename={nome_grafico}.png"}
    )

@router.get("/download/excel/{ano}")
async def download_excel(
    ano: int,
    db: Session = Depends(get_session)
):
    """Download do relatório em Excel"""
    try:
        analytics = ServidorAnalytics(db)
        relatorio = analytics.gerar_relatorio_completo(ano)
        
        # Gerar arquivo Excel
        nome_arquivo = f"relatorio_servidores_{ano}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        caminho_arquivo = os.path.join("static", "exports", nome_arquivo)
        
        # Criar diretório se não existir
        os.makedirs(os.path.dirname(caminho_arquivo), exist_ok=True)
        
        analytics.exportar_dados_excel(relatorio, caminho_arquivo)
        
        return FileResponse(
            caminho_arquivo,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename=nome_arquivo
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao gerar Excel: {str(e)}")

@router.get("/comparativo/{ano1}/{ano2}")
async def comparativo_anos(
    ano1: int,
    ano2: int,
    db: Session = Depends(get_session)
):
    try:
        analytics = ServidorAnalytics(db)
        
        relatorio1 = analytics.gerar_relatorio_completo(ano1)
        analytics.insights.clear()  
        relatorio2 = analytics.gerar_relatorio_completo(ano2)
        
        # Calcular diferenças
        diferenca_servidores = (
            relatorio2['resumo_geral']['servidores_ativos'] - 
            relatorio1['resumo_geral']['servidores_ativos']
        )
        
        diferenca_remuneracao = (
            relatorio2['resumo_geral']['media_remuneracao'] - 
            relatorio1['resumo_geral']['media_remuneracao']
        )
        
        percentual_variacao = (diferenca_remuneracao / relatorio1['resumo_geral']['media_remuneracao']) * 100
        
        return {
            "periodo_comparacao": f"{ano1} vs {ano2}",
            "resumo_comparativo": {
                "diferenca_servidores_ativos": diferenca_servidores,
                "diferenca_remuneracao_media": round(diferenca_remuneracao, 2),
                "percentual_variacao_remuneracao": round(percentual_variacao, 2)
            },
            "ano1": {
                "ano": ano1,
                "resumo": relatorio1['resumo_geral']
            },
            "ano2": {
                "ano": ano2,
                "resumo": relatorio2['resumo_geral']
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro na comparação: {str(e)}")

# Funções auxiliares
def _gerar_excel_background(analytics: ServidorAnalytics, relatorio: dict, ano: int):
    """Gera Excel em background task"""
    nome_arquivo = f"static/exports/relatorio_{ano}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    os.makedirs(os.path.dirname(nome_arquivo), exist_ok=True)
    analytics.exportar_dados_excel(relatorio, nome_arquivo)

def _gerar_grafico_especifico(analytics: ServidorAnalytics, ano: int, tipo: str):
    """Gera gráfico específico baseado no tipo"""
    graficos_map = {
        "remuneracao": analytics._grafico_evolucao_remuneracao,
        "cargos": analytics._grafico_remuneracao_por_cargo,
        "afastamentos": analytics._grafico_afastamentos_mensais,
        "organizacional": analytics._grafico_distribuicao_organizacional,
        "correlacao": analytics._grafico_remuneracao_vs_afastamentos
    }
    
    if tipo in graficos_map:
        if tipo == "organizacional":
            graficos_map[tipo]()
            return "distribuicao_organizacional.png"
        else:
            graficos_map[tipo](ano)
            
        # Mapear tipos para nomes de arquivo
        arquivo_map = {
            "remuneracao": "evolucao_remuneracao_mensal.png",
            "cargos": "remuneracao_por_cargo.png", 
            "afastamentos": "afastamentos_por_mes.png",
            "correlacao": "remuneracao_vs_afastamentos.png"
        }
        
        return arquivo_map.get(tipo, f"{tipo}_{ano}.png")
    
    return None
  
@router.get("/download/estatisticas-csv/{ano}")
async def download_estatisticas_csv(
    ano: int,
    incluir_detalhes: bool = Query(True, description="Incluir análises detalhadas"),
    agrupar_por: str = Query("cargo", description="Agrupar por: cargo, orgao, mes"),
    db: Session = Depends(get_session)
):
    """Download de relatório estatístico completo em CSV"""
    try:
        from sqlalchemy import text
        import csv
        import io
        from datetime import datetime
        import statistics
        import numpy as np
        
        # Validar parâmetro de agrupamento
        agrupamentos_validos = ["cargo", "orgao", "mes", "servidor"]
        if agrupar_por not in agrupamentos_validos:
            raise HTTPException(
                status_code=400, 
                detail=f"Agrupamento inválido. Use: {', '.join(agrupamentos_validos)}"
            )
        
        # Nome do arquivo com timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        nome_arquivo = f"estatisticas_servidores_{ano}_{agrupar_por}_{timestamp}.csv"
        caminho_arquivo = os.path.join("static", "exports", nome_arquivo)
        
        # Criar diretório se não existir
        os.makedirs(os.path.dirname(caminho_arquivo), exist_ok=True)
        
        # Gerar dados estatísticos
        dados_estatisticos = await _gerar_estatisticas_completas(db, ano, agrupar_por, incluir_detalhes)
        
        # Criar arquivo CSV
        with open(caminho_arquivo, 'w', newline='', encoding='utf-8-sig') as csvfile:
            writer = csv.writer(csvfile, delimiter=';')  # Usar ponto e vírgula para compatibilidade com Excel BR
            
            # Cabeçalho do relatório
            writer.writerow([f"RELATÓRIO ESTATÍSTICO - SERVIDORES PÚBLICOS - ANO {ano}"])
            writer.writerow([f"Gerado em: {datetime.now().strftime('%d/%m/%Y às %H:%M:%S')}"])
            writer.writerow([f"Agrupamento: {agrupar_por.upper()}"])
            writer.writerow([])  # Linha em branco
            
            # 1. RESUMO EXECUTIVO
            writer.writerow([" RESUMO EXECUTIVO "])
            resumo = dados_estatisticos["resumo_executivo"]
            for chave, valor in resumo.items():
                label = chave.replace('_', ' ').title()
                if isinstance(valor, float):
                    valor_formatado = f"{valor:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
                else:
                    valor_formatado = str(valor)
                writer.writerow([label, valor_formatado])
            writer.writerow([])
            
            # 2. ESTATÍSTICAS DESCRITIVAS
            writer.writerow([" ESTATÍSTICAS DESCRITIVAS"])
            estatisticas = dados_estatisticos["estatisticas_descritivas"]
            writer.writerow(["Métrica", "Valor"])
            for metrica, valor in estatisticas.items():
                label = metrica.replace('_', ' ').title()
                if isinstance(valor, (int, float)):
                    valor_formatado = f"R$ {valor:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.') if 'remuneracao' in metrica.lower() else f"{valor:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
                else:
                    valor_formatado = str(valor)
                writer.writerow([label, valor_formatado])
            writer.writerow([])
            
            # 3. ANÁLISE POR AGRUPAMENTO
            writer.writerow([f"ANÁLISE POR {agrupar_por.upper()}"])
            analise_grupo = dados_estatisticos["analise_por_grupo"]
            
            if analise_grupo:
                # Cabeçalhos das colunas
                primeira_linha = next(iter(analise_grupo.values()))
                colunas = list(primeira_linha.keys())
                colunas_formatadas = [col.replace('_', ' ').title() for col in colunas]
                writer.writerow([agrupar_por.title()] + colunas_formatadas)
                
                # Dados por grupo
                for grupo, dados_grupo in analise_grupo.items():
                    linha = [grupo]
                    for coluna in colunas:
                        valor = dados_grupo[coluna]
                        if isinstance(valor, float):
                            if 'remuneracao' in coluna.lower() or 'salario' in coluna.lower():
                                valor_formatado = f"R$ {valor:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
                            else:
                                valor_formatado = f"{valor:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
                        else:
                            valor_formatado = str(valor)
                        linha.append(valor_formatado)
                    writer.writerow(linha)
            writer.writerow([])
            
            # 4. ANÁLISE DE QUARTIS E PERCENTIS
            if "quartis_percentis" in dados_estatisticos:
                writer.writerow(["ANÁLISE DE QUARTIS E PERCENTIS"])
                quartis = dados_estatisticos["quartis_percentis"]
                writer.writerow(["Percentil", "Valor da Remuneração"])
                for percentil, valor in quartis.items():
                    valor_formatado = f"R$ {valor:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
                    writer.writerow([percentil, valor_formatado])
                writer.writerow([])
            
            # 5. DADOS DETALHADOS (se solicitado)
            if incluir_detalhes and "dados_detalhados" in dados_estatisticos:
                writer.writerow([" DADOS DETALHADOS "])
                dados_detalhados = dados_estatisticos["dados_detalhados"]
                
                if dados_detalhados:
                    # Cabeçalhos
                    primeira_linha = dados_detalhados[0]
                    colunas = list(primeira_linha.keys())
                    colunas_formatadas = [col.replace('_', ' ').title() for col in colunas]
                    writer.writerow(colunas_formatadas)
                    
                    # Dados linha por linha
                    for linha_dados in dados_detalhados:
                        linha = []
                        for coluna in colunas:
                            valor = linha_dados[coluna]
                            if isinstance(valor, float) and ('remuneracao' in coluna.lower() or 'salario' in coluna.lower()):
                                valor_formatado = f"R$ {valor:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
                            else:
                                valor_formatado = str(valor) if valor is not None else "N/A"
                            linha.append(valor_formatado)
                        writer.writerow(linha)
                writer.writerow([])
            
            # 6. CORRELAÇÕES (se disponível)
            if "correlacoes" in dados_estatisticos:
                writer.writerow([" ANÁLISE DE CORRELAÇÕES "])
                correlacoes = dados_estatisticos["correlacoes"]
                writer.writerow(["Variável 1", "Variável 2", "Coeficiente de Correlação", "Interpretação"])
                for corr_data in correlacoes:
                    interpretacao = _interpretar_correlacao(corr_data["coeficiente"])
                    writer.writerow([
                        corr_data["variavel1"],
                        corr_data["variavel2"], 
                        f"{corr_data['coeficiente']:.4f}",
                        interpretacao
                    ])
                writer.writerow([])
            
            # 7. OBSERVAÇÕES E METODOLOGIA
            writer.writerow([" OBSERVAÇÕES E METODOLOGIA "])
            writer.writerow(["• Valores monetários em reais (R$)"])
            writer.writerow(["• Separador decimal: vírgula (,)"])
            writer.writerow(["• Separador de milhares: ponto (.)"])
            writer.writerow(["• Dados baseados em registros de remuneração válidos"])
            writer.writerow(["• Estatísticas calculadas apenas para servidores ativos no período"])
            writer.writerow([f"• Período de análise: {ano}"])
            writer.writerow([f"• Total de registros analisados: {dados_estatisticos.get('total_registros', 'N/A')}"])
        
        return FileResponse(
            caminho_arquivo,
            media_type="text/csv",
            filename=nome_arquivo,
            headers={
                "Content-Disposition": f"attachment; filename={nome_arquivo}",
                "Content-Type": "text/csv; charset=utf-8"
            }
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao gerar CSV estatístico: {str(e)}")

async def _gerar_estatisticas_completas(db: Session, ano: int, agrupar_por: str, incluir_detalhes: bool) -> dict:
    """Gera todas as estatísticas necessárias para o relatório CSV"""
    from sqlalchemy import text
    import statistics
    import numpy as np
    
    resultado = {}
    
    # 1. RESUMO EXECUTIVO
    resumo_query = text("""
        SELECT 
            COUNT(DISTINCT r.id_servidor) as servidores_ativos,
            COUNT(r.id_servidor) as total_registros,
            SUM(r.remuneracao_final) as total_remuneracao,
            AVG(r.remuneracao_final) as media_remuneracao,
            MIN(r.remuneracao_final) as menor_remuneracao,
            MAX(r.remuneracao_final) as maior_remuneracao
        FROM remuneracoes r
        WHERE r.ano = :ano AND r.remuneracao_final IS NOT NULL AND r.remuneracao_final > 0
    """)
    
    resumo_result = db.execute(resumo_query, {"ano": ano}).fetchone()
    
    resultado["resumo_executivo"] = {
        "servidores_ativos": resumo_result.servidores_ativos or 0,
        "total_registros": resumo_result.total_registros or 0,
        "total_remuneracao": float(resumo_result.total_remuneracao or 0),
        "media_remuneracao": float(resumo_result.media_remuneracao or 0),
        "menor_remuneracao": float(resumo_result.menor_remuneracao or 0),
        "maior_remuneracao": float(resumo_result.maior_remuneracao or 0)
    }
    resultado["total_registros"] = resumo_result.total_registros
    
    # 2. ESTATÍSTICAS DESCRITIVAS AVANÇADAS
    if resumo_result.total_registros > 0:
        # Buscar todos os valores de remuneração para cálculos estatísticos
        valores_query = text("""
            SELECT remuneracao_final 
            FROM remuneracoes 
            WHERE ano = :ano AND remuneracao_final IS NOT NULL AND remuneracao_final > 0
            ORDER BY remuneracao_final
        """)
        
        valores_result = db.execute(valores_query, {"ano": ano}).fetchall()
        valores = [float(row.remuneracao_final) for row in valores_result]
        
        if valores:
            # Calcular estatísticas descritivas
            resultado["estatisticas_descritivas"] = {
                "media_remuneracao": statistics.mean(valores),
                "mediana_remuneracao": statistics.median(valores),
                "moda_remuneracao": statistics.mode(valores) if len(set(valores)) < len(valores) else "N/A",
                "desvio_padrao": statistics.stdev(valores) if len(valores) > 1 else 0,
                "variancia": statistics.variance(valores) if len(valores) > 1 else 0,
                "amplitude": max(valores) - min(valores),
                "coeficiente_variacao": (statistics.stdev(valores) / statistics.mean(valores)) * 100 if len(valores) > 1 and statistics.mean(valores) > 0 else 0
            }
            
            # Quartis e percentis
            resultado["quartis_percentis"] = {
                "P10": np.percentile(valores, 10),
                "Q1 (P25)": np.percentile(valores, 25),
                "Q2 (P50 - Mediana)": np.percentile(valores, 50),
                "Q3 (P75)": np.percentile(valores, 75),
                "P90": np.percentile(valores, 90),
                "P95": np.percentile(valores, 95),
                "P99": np.percentile(valores, 99)
            }
    
    # 3. ANÁLISE POR AGRUPAMENTO
    resultado["analise_por_grupo"] = await _gerar_analise_por_grupo(db, ano, agrupar_por)
    
    # 4. DADOS DETALHADOS (se solicitado)
    if incluir_detalhes:
        resultado["dados_detalhados"] = await _gerar_dados_detalhados(db, ano, agrupar_por)
    
    # 5. ANÁLISE DE CORRELAÇÕES
    resultado["correlacoes"] = await _gerar_correlacoes(db, ano)
    
    return resultado

async def _gerar_analise_por_grupo(db: Session, ano: int, agrupar_por: str) -> dict:
    """Gera análise estatística agrupada"""
    from sqlalchemy import text
    
    # Mapear agrupamentos para colunas SQL
    mapeamento_colunas = {
        "cargo": "s.descr_cargo",
        "orgao": "s.org_exercicio", 
        "mes": "r.mes",
        "servidor": "CONCAT(s.nome, ' (ID: ', s.id_servidor, ')')"
    }
    
    if agrupar_por not in mapeamento_colunas:
        return {}
    
    coluna_agrupamento = mapeamento_colunas[agrupar_por]
    
    query = text(f"""
        SELECT 
            {coluna_agrupamento} as grupo,
            COUNT(DISTINCT r.id_servidor) as servidores_unicos,
            COUNT(r.id_servidor) as total_registros,
            AVG(r.remuneracao_final) as media_remuneracao,
            MIN(r.remuneracao_final) as menor_remuneracao,
            MAX(r.remuneracao_final) as maior_remuneracao,
            SUM(r.remuneracao_final) as total_remuneracao
        FROM remuneracoes r
        LEFT JOIN servidores s ON r.id_servidor = s.id_servidor
        WHERE r.ano = :ano AND r.remuneracao_final IS NOT NULL AND r.remuneracao_final > 0
        GROUP BY {coluna_agrupamento}
        ORDER BY media_remuneracao DESC
        LIMIT 50
    """)
    
    resultados = db.execute(query, {"ano": ano}).fetchall()
    
    analise = {}
    for row in resultados:
        grupo_nome = str(row.grupo) if row.grupo else "N/A"
        analise[grupo_nome] = {
            "servidores_unicos": row.servidores_unicos,
            "total_registros": row.total_registros,
            "media_remuneracao": float(row.media_remuneracao or 0),
            "menor_remuneracao": float(row.menor_remuneracao or 0),
            "maior_remuneracao": float(row.maior_remuneracao or 0),
            "total_remuneracao": float(row.total_remuneracao or 0),
            "amplitude_salarial": float((row.maior_remuneracao or 0) - (row.menor_remuneracao or 0))
        }
    
    return analise

async def _gerar_dados_detalhados(db: Session, ano: int, agrupar_por: str) -> list:
    """Gera dados detalhados para incluir no CSV"""
    from sqlalchemy import text
    
    # Limitar quantidade de registros para não sobrecarregar o CSV
    limite_registros = 1000
    
    query = text("""
        SELECT 
            s.id_servidor,
            s.nome,
            s.descr_cargo,
            s.org_exercicio,
            r.mes,
            r.remuneracao_final,
            r.ano
        FROM remuneracoes r
        LEFT JOIN servidores s ON r.id_servidor = s.id_servidor
        WHERE r.ano = :ano AND r.remuneracao_final IS NOT NULL
        ORDER BY r.remuneracao_final DESC
        LIMIT :limite
    """)
    
    resultados = db.execute(query, {"ano": ano, "limite": limite_registros}).fetchall()
    
    dados_detalhados = []
    for row in resultados:
        dados_detalhados.append({
            "id_servidor": row.id_servidor,
            "nome_servidor": row.nome or "N/A",
            "cargo": row.descr_cargo or "N/A", 
            "orgao": row.org_exercicio or "N/A",
            "mes": row.mes,
            "remuneracao_final": float(row.remuneracao_final or 0),
            "ano": row.ano
        })
    
    return dados_detalhados

async def _gerar_correlacoes(db: Session, ano: int) -> list:
    """Gera análise de correlações entre variáveis"""
    from sqlalchemy import text
    import numpy as np
    
    try:
        # Buscar dados para análise de correlação
        query = text("""
            SELECT 
                r.remuneracao_final,
                r.mes,
                COUNT(a.id_afastamento) as total_afastamentos
            FROM remuneracoes r
            LEFT JOIN afastamentos a ON r.id_servidor = a.id_servidor AND r.ano = a.ano AND r.mes = a.mes
            WHERE r.ano = :ano AND r.remuneracao_final IS NOT NULL
            GROUP BY r.id_servidor, r.mes, r.remuneracao_final
            HAVING COUNT(*) > 0
            LIMIT 500
        """)
        
        resultados = db.execute(query, {"ano": ano}).fetchall()
        
        if len(resultados) < 10:  # Precisa de dados suficientes para correlação
            return []
        
        # Extrair dados para arrays numpy
        remuneracoes = np.array([float(row.remuneracao_final) for row in resultados])
        meses = np.array([int(row.mes) for row in resultados])
        afastamentos = np.array([int(row.total_afastamentos) for row in resultados])
        
        correlacoes = []
        
        # Correlação entre remuneração e mês
        if len(set(meses)) > 1:
            corr_rem_mes = np.corrcoef(remuneracoes, meses)[0, 1]
            if not np.isnan(corr_rem_mes):
                correlacoes.append({
                    "variavel1": "Remuneração",
                    "variavel2": "Mês",
                    "coeficiente": float(corr_rem_mes)
                })
        
        # Correlação entre remuneração e afastamentos
        if len(set(afastamentos)) > 1:
            corr_rem_afas = np.corrcoef(remuneracoes, afastamentos)[0, 1]
            if not np.isnan(corr_rem_afas):
                correlacoes.append({
                    "variavel1": "Remuneração", 
                    "variavel2": "Afastamentos",
                    "coeficiente": float(corr_rem_afas)
                })
        
        return correlacoes
        
    except Exception as e:
        print(f"Erro ao calcular correlações: {e}")
        return []

def _interpretar_correlacao(coeficiente: float) -> str:
    abs_coef = abs(coeficiente)
    
    if abs_coef >= 0.9:
        intensidade = "muito forte"
    elif abs_coef >= 0.7:
        intensidade = "forte"
    elif abs_coef >= 0.5:
        intensidade = "moderada"
    elif abs_coef >= 0.3:
        intensidade = "fraca"
    else:
        intensidade = "muito fraca"
    
    direcao = "positiva" if coeficiente > 0 else "negativa"
    
    return f"Correlação {intensidade} {direcao}"