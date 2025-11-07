# 🎧 Análise de Popularidade de Músicas do Spotify (Pós-2015)

Este projeto utiliza o **Databricks** e **Spark SQL** para realizar uma pipeline de **ETL/ELT** e analisar dados do Spotify, com o objetivo de identificar as músicas e artistas que dominaram o cenário musical nos anos mais recentes (a partir de 2016).

## 🚀 Tecnologias Utilizadas

* **Ambiente:** Databricks Community Edition
* **Processamento:** Apache Spark
* **Linguagem Principal:** PySpark / Spark SQL

## 📊 Fonte de Dados

O dataset utilizado é o `spotify_data_1.csv`, contendo diversas métricas sobre músicas (popularidade, danceability, tempo, etc.) ao longo dos anos.

## 🛠️ Código e Análises Principais

O código completo das etapas de carregamento e consulta está no arquivo **`Spotify data from PySpark.py`**. As três análises principais realizadas via Spark SQL foram:

### 1. As Músicas Mais Populares (Pós-2015)

Identificação das faixas com maior popularidade acumulada lançadas na última década.

**Query:**
```sql
SELECT name, artists, SUM(popularity) AS Popularidade_total
FROM spotify_data_1_csv
WHERE YEAR >= 2015
GROUP BY name, artists
ORDER BY Popularidade_total DESC

