# BCB Data Pipeline

Pipeline de dados desenvolvido utilizando séries históricas do **Banco Central do Brasil (BCB)**, com foco em ingestão incremental, processamento de dados e construção de uma camada analítica.

## Arquitetura

O projeto utiliza uma arquitetura baseada em **Data Lake + Data Warehouse**, organizada em camadas:

**BCB API → Airflow → Python → MinIO (Bronze/Silver/Gold) → PostgreSQL → dbt**

### Tecnologias

| Tecnologia | Função                                |
| ---------- | ------------------------------------- |
| Python     | Ingestão dos dados da API             |
| Airflow    | Orquestração do pipeline              |
| Spark      | Processamento e transformação         |
| MinIO      | Data Lake                             |
| Parquet    | Armazenamento das camadas processadas |
| PostgreSQL | Data Warehouse                        |
| dbt        | Transformações e modelagem            |
| Docker     | Containerização                       |

## Camadas

### Bronze

Armazena os dados recebidos diretamente da API do BCB em formato JSON.

Os dados são organizados por:

* Série
* Data de ingestão
* `batch_id`

O `batch_id` permite identificar qual execução do Airflow gerou determinado lote.

### Silver

Responsável pelo tratamento e estruturação dos dados utilizando Apache Spark.

Os dados são armazenados em Parquet e particionados por ano e mês.

Principais operações:

* Conversão de tipos;
* Tratamento de datas;
* Remoção de duplicidades;
* Controle de registros já existentes;
* Carga incremental em modo append.

### Gold

Camada destinada ao consumo analítico.

Os indicadores são consolidados em nível mensal e armazenados em Parquet, particionados por mês de referência.

### PostgreSQL

Os dados consolidados da Gold são carregados no PostgreSQL, formando a camada de Data Warehouse.

A principal tabela analítica é:

`gold.macro_monthly`

### dbt

O dbt é utilizado para realizar as transformações finais dentro do PostgreSQL, além de possibilitar a criação de modelos, testes e documentação dos dados.

## Ingestão Incremental

A ingestão utiliza uma **watermark baseada na data de referência do BCB**.

O estado de cada série mantém a última data processada. Nas execuções seguintes, somente observações posteriores a essa data são consideradas para ingestão.

Esse processo evita o processamento desnecessário de todo o histórico e permite manter os dados anteriores sem sobrescrevê-los.

## Orquestração

O pipeline é executado pelo Apache Airflow seguindo as seguintes etapas:

1. Ingestão dos dados do BCB;
2. Bronze → Silver;
3. Silver → Gold;
4. Gold → PostgreSQL;
5. Execução dos modelos dbt.

## Infraestrutura

Todo o ambiente é executado utilizando Docker Compose, permitindo executar os principais componentes do projeto de forma isolada e reproduzível.

## Objetivo

O projeto tem como objetivo aplicar conceitos de **Engenharia de Dados** em um pipeline completo, incluindo:

* Ingestão de APIs;
* Processamento incremental;
* Data Lake;
* Arquitetura Bronze, Silver e Gold;
* Particionamento;
* Processamento com Spark;
* Orquestração com Airflow;
* Data Warehouse;
* Transformações com dbt;
* Containerização com Docker.
