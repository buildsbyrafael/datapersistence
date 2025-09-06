# Sistema de Análise e Gestão de Servidores Públicos

Este projeto é um backend robusto, para a gestão, análise e visualização de dados de servidores públicos. Ele foi projetado para importar grandes volumes de dados de arquivos CSV e oferecer uma API completa para consulta, análise estatística e geração de relatórios.

### Estrutura e Componentes Principais

O sistema é modular e organizado em torno de componentes-chave, que trabalham juntos para gerenciar o fluxo de dados e as operações.

- **API (/app/api):** A camada de API define todos os endpoints para interação com o sistema. É responsável por receber requisições, validar dados e chamar as funções de negócio correspondentes.
- **Modelos de Dados (/app/models):** As classes de modelos, definidas com **SQLModel**, representam a estrutura do banco de dados e os esquemas de dados da aplicação. Isso inclui entidades como **Servidor**, **Remuneracao**, **Afastamento**, **CargoFuncao**, **Observacao** e suas relações.
- **Operações de Repositório (/app/crud):** Contém as funções de **Create**, **Read**, **Update** e **Delete**, que interagem diretamente com o banco de dados. Essas funções encapsulam a lógica de persistência de dados.
- **Análise (/app/analytics):** Este módulo é o motor analítico do sistema. Ele processa dados para gerar resumos, estatísticas, insights e gráficos. Possui endpoints para gerar relatórios completos, comparar dados de meses diferentes e exportar dados em formatos como Excel e CSV.
- **Utilitários de Importação (/app/utils/importar_*.py):** Scripts utilitários que gerenciam a importação de dados de arquivos CSV para o banco de dados. Eles incluem a lógica de limpeza, validação e inserção de dados em massa.
- **Configuração (/app/core/config.py):** Centraliza as configurações do aplicativo, como as credenciais do banco de dados, usando variáveis de ambiente.

### Técnicas de Programação e Padrões de Projeto

O sistema utiliza diversas técnicas de programação e padrões de design para garantir robustez, escalabilidade e manutenibilidade.

- **Pydantic:** As classes de modelos (**schemas**) são definidas com **Pydantic** para validação de dados em tempo de execução. Isso garante que os dados recebidos via API estejam corretos antes de serem processados.
- **Injeção de Dependência:** O FastAPI utiliza injeção de dependência (**Depends**) para gerenciar sessões do banco de dados (**get_session()**), o que simplifica a lógica de cada endpoint e facilita testes.
- **Modularidade e Componentização:** Cada parte do sistema tem uma responsabilidade clara e isolada. As rotas da API, as operações de CRUD e a lógica de análise estão em arquivos e diretórios separados, tornando o código mais organizado e fácil de manter.
- **Operações Assíncronas (async/await):** O FastAPI utiliza operações assíncronas para lidar com requisições de forma não-bloqueante, o que melhora o desempenho da API, especialmente em operações que envolvem I/O, como a leitura de arquivos e acesso ao banco de dados.
- **Tarefas em Segundo Plano (BackgroundTasks):** Tarefas pesadas, como a geração de um relatório completo, são executadas em segundo plano para que a API possa retornar uma resposta imediata ao usuário e evitar timeouts.

### Tecnologias e Linguagens

[![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![Visual Studio](https://img.shields.io/badge/Visual%20Studio-5C2D91?style=for-the-badge&logo=visual-studio&logoColor=white)](https://visualstudio.microsoft.com/pt-br/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white)](https://www.postgresql.org/)
