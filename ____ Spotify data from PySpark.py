# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

# MAGIC %md
# MAGIC Com o crescimento explosivo dos serviços de streaming, milhões de músicas são consumidas diariamente em plataformas digitais. Cada faixa carrega consigo informações valiosas: título, artista, data de lançamento, métricas de popularidade e até características musicais como energia, valência e duração. Esse tipo de dado é exatamente o que empresas como Spotify, Deezer e Apple Music utilizam para entender preferências do público e prever tendências. Pensando nisso, desenvolvi um projeto prático de ETL e ELT no Databricks, usando um dataset musical realista — semelhante a uma extração de catálogo do Spotify.
# MAGIC
# MAGIC                     

# COMMAND ----------

# MAGIC %md Etapa ETL — Preparação e transformação inicial
# MAGIC
# MAGIC Primeiro, utilizei arquivos CSV armazenados no DBFS. Fiz a extração, limpeza e padronização de colunas, garantindo que:
# MAGIC
# MAGIC - Datas fossem reconhecidas corretamente,
# MAGIC
# MAGIC - Ppularidade fosse tratada como campo numérico,
# MAGIC
# MAGIC - Listas de artistas se tornassem estruturas consistentes,
# MAGIC
# MAGIC - Valores ausentes fossem ajustados,
# MAGIC
# MAGIC - Tudo estivesse pronto para análise.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Etapa ELT — Transformações analíticas via SQL
# MAGIC
# MAGIC Com os dados já carregados no Lakehouse, apliquei transformações diretamente via SQL no Databricks, criando tabelas analíticas otimizadas.
# MAGIC
# MAGIC E então, três perguntas centrais que os dados transformados deveriam responder — perguntas muito comuns para empresas que analisam tendências musicais: 

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/spotify_data-1.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

df = spark.read.format(file_type) \
    .option("inferSchema", infer_schema) \
    .option("header", first_row_is_header) \
    .option("delimiter", delimiter) \
    .load(file_location)

display(df)


# COMMAND ----------

# Create a view or table
temp_table_name = "spotify_data_1_csv"

df.createOrReplaceTempView(temp_table_name)


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC /* Query the created temp table in a SQL cell */
# MAGIC
# MAGIC select * from `spotify_data_1_csv`

# COMMAND ----------

# With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.

permanent_table_name = "spotify_data_1_csv"

# df.write.format("parquet").saveAsTable(permanent_table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC 1. As músicas mais populares nos últimos 10 anos;
# MAGIC
# MAGIC Aqui extraí as faixas lançadas na última década e ordenei pela métrica oficial de popularidade.
# MAGIC O objetivo: identificar quem realmente dominou o mercado nesse período.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT name, 
# MAGIC        artists,  
# MAGIC        SUM (popularity) As Popularidade_total 
# MAGIC FROM 
# MAGIC       spotify_data_1_csv
# MAGIC WHERE
# MAGIC       year >= 2015
# MAGIC GROUP BY name, artists
# MAGIC ORDER BY Popularidade_total  DESC

# COMMAND ----------

# MAGIC %md
# MAGIC 2. A quantidade de músicas lançadas por artista e ano de lançamento: 
# MAGIC
# MAGIC Essa análise permitiu visualizar a produtividade de cada artista ao longo dos anos.
# MAGIC Detectando padrões interessantes, como artistas que lançam consistentemente e outros com picos específicos de atividade.        

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT artists,
# MAGIC        year,
# MAGIC        COUNT (name) AS Quantidade_musicas
# MAGIC FROM  spotify_data_1_csv  
# MAGIC     
# MAGIC GROUP BY artists, year 
# MAGIC ORDER BY 
# MAGIC  Quantidade_musicas DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Os artistas com a maior média de popularidade de músicas nos últimos 5 anos.
# MAGIC
# MAGIC Nessa etapa foquei em consistência: não só quem lança músicas, mas quem lança músicas bem-sucedidas.
# MAGIC Com isso, foi possível identificar artistas que mantêm alta popularidade no cenário recente.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT artists,
# MAGIC AVG (popularity) As media_popularidade
# MAGIC FROM spotify_data_1_csv
# MAGIC WHERE year >= 2020
# MAGIC GROUP BY artists
# MAGIC ORDER BY media_popularidade DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC Esse projeto demonstra, de forma prática, como transformar dados musicais brutos em insights reais usando um pipeline de ETL + ELT no Databricks.
# MAGIC Os dados do dataset se encaixam perfeitamente nessas análises e permitem construir visualizações e conclusões que agregam muito ao portfólio.