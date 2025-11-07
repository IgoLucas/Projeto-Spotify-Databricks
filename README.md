# 🎧 Análise de Popularidade de Músicas do Spotify (ETL/ELT no Databricks)

Com o crescimento explosivo dos serviços de streaming, milhões de músicas são consumidas diariamente em plataformas digitais. Cada faixa carrega consigo informações valiosas: título, artista, data de lançamento, métricas de popularidade e até características musicais como energia, valência e duração. Esse tipo de dado é exatamente o que empresas como Spotify, Deezer e Apple Music utilizam para entender preferências do público e prever tendências.

Pensando nisso, desenvolvi um projeto prático de **ETL e ELT no Databricks**, usando um dataset musical realista — semelhante a uma extração de catálogo do Spotify.

O dataset contém informações como:
* Nome da música
* Artistas
* Data de lançamento
* Popularidade
* Características musicais (danceability, energy, acousticness etc.)
* Duração da faixa

Esse foi o ponto de partida para a construção de um pipeline completo dentro do Lakehouse.

## 🛠️ Pipeline e Análises

### ✅ Etapa ETL — Preparação e transformação inicial

Primeiro, utilizei arquivos CSV armazenados no DBFS. Fiz a extração, limpeza e padronização de colunas, garantindo que:
* datas fossem reconhecidas corretamente,
* popularidade fosse tratada como campo numérico,
* listas de artistas se tornassem estruturas consistentes,
* valores ausentes fossem ajustados,
* e tudo estivesse pronto para análise.

### ✅ Etapa ELT — Transformações analíticas via SQL

Com os dados já carregados no Lakehouse, apliquei transformações diretamente via SQL no Databricks, criando tabelas analíticas otimizadas.
E então, defini três perguntas centrais que os dados transformados deveriam responder — perguntas muito comuns para empresas que analisam tendências musicais:

**1. As músicas mais populares nos últimos 10 anos;**
Aqui extraí as faixas lançadas na última década e ordenei pela métrica oficial de popularidade. O objetivo: identificar quem realmente dominou o mercado nesse período.

**2. A quantidade de músicas lançadas por artista e ano de lançamento;**
Essa análise permitiu visualizar a produtividade de cada artista ao longo dos anos. Detectamos padrões interessantes, como artistas que lançam consistentemente e outros com picos específicos de atividade.

**3. Os artistas com a maior média de popularidade de músicas nos últimos 5 anos.**
Nessa etapa foquei em consistência: não só quem lança músicas, mas quem lança músicas bem-sucedidas. Com isso, foi possível identificar artistas que mantêm alta popularidade no cenário recente.

## ✨ Resultados e Insights

* **Música Mais Popular (Pós-2015):** **"Toosie Slide"** do artista **Drake**, com uma Popularidade Total de **251**.
* **Artistas de Destaque:** Drake e Dua Lipa se destacaram no topo da lista das faixas mais populares pós-2015.

### ✅ Conclusão

Esse projeto demonstra, de forma prática, como transformar dados musicais brutos em insights reais usando um pipeline de ETL + ELT no Databricks. Os dados do dataset se encaixam perfeitamente nessas análises e permitem construir visualizações e conclusões que agregam muito ao portfólio.



