# BCB Data Pipeline

Pipeline de dados desenvolvido utilizando séries históricas do **Banco Central do Brasil (BCB)**, com foco em ingestão incremental, processamento distribuído, armazenamento em Data Lake e construção de uma camada analítica.

## Arquitetura

O projeto utiliza uma arquitetura baseada em **Data Lake + Data Warehouse**, organizada em camadas:

**BCB API → Airflow → Python → MinIO (Bronze/Silver/Gold) → PostgreSQL → dbt**

A ingestão foi estruturada para garantir **processamento incremental, consistência dos batches e publicação atômica dos dados**.

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

A Bronze armazena os dados recebidos diretamente da API do BCB em formato JSON.

Os dados são organizados por:

* Série;
* Data de ingestão;
* `batch_id`.

Exemplo:

```text
<series>/
└── ingestion_date=YYYY-MM-DD/
    └── batch_id=<batch_id>/
        └── response.json
```

O `batch_id` identifica a execução responsável pela geração daquele lote.

A Bronze utiliza uma área de **staging** antes da publicação definitiva:

```text
_staging/
└── ingestion_date=YYYY-MM-DD/
    └── batch_id=<batch_id>/
        └── <series>/
            └── response.json
```

Durante a ingestão, os dados são primeiro construídos nessa área temporária. Somente após todas as séries serem processadas com sucesso o batch é publicado na Bronze.

Além disso, cada batch possui um **commit marker**:

```text
_control/
└── batches/
    └── ingestion_date=YYYY-MM-DD/
        └── batch_id=<batch_id>.json
```

Esse marcador identifica que o batch foi completamente processado e está disponível para as etapas seguintes.

Dessa forma, batches incompletos ou que apresentaram falha durante a ingestão não são considerados pela camada Silver.

### Silver

A Silver é responsável pelo tratamento e estruturação dos dados utilizando **Apache Spark**.

Os dados são armazenados em Parquet e particionados por ano e mês.

Principais operações:

* Conversão de tipos;
* Tratamento de datas;
* Padronização dos registros;
* Remoção de duplicidades;
* Controle de registros já existentes;
* Carga incremental em modo append.

O processamento também verifica os registros que já existem na Silver, evitando a inserção de observações duplicadas.

### Gold

A Gold é destinada ao consumo analítico.

Os indicadores são consolidados em nível mensal e armazenados em Parquet, particionados por mês de referência.

Essa camada concentra os dados preparados para análises e consumo pelo Data Warehouse.

### PostgreSQL

Os dados consolidados da Gold são carregados no PostgreSQL, formando a camada de **Data Warehouse**.

A principal tabela analítica é:

```text
gold.macro_monthly
```

### dbt

O dbt é utilizado para realizar as transformações finais dentro do PostgreSQL.

A camada permite:

* Criação de modelos analíticos;
* Transformações SQL;
* Testes de qualidade;
* Documentação;
* Organização das dependências entre modelos.

## Ingestão Incremental

A ingestão utiliza uma **watermark baseada na data de referência do BCB**.

Cada série possui um estado de controle armazenado na Bronze:

```text
_control/
└── <series>.json
```

Esse estado mantém informações como:

* Última data de referência processada;
* Última data de ingestão;
* `batch_id` do último batch publicado;
* Quantidade de registros processados.

Nas execuções seguintes, a pipeline consulta a API a partir da última referência conhecida e considera somente observações posteriores à watermark.

### Snapshot completo por batch

Embora a consulta à API seja incremental, cada batch publicado na Bronze representa um **snapshot completo de todas as séries**.

Quando uma série possui novos registros:

```text
API
 ↓
Novos registros
 ↓
Staging
 ↓
Novo snapshot Bronze
```

Quando uma série não possui novos registros, o pipeline reutiliza fisicamente o conteúdo do último batch publicado:

```text
Último batch Bronze
 ↓
Copy
 ↓
Novo staging
 ↓
Novo snapshot Bronze
```

Isso garante que cada batch possua uma visão completa e consistente das séries, mesmo quando apenas parte delas recebeu novas observações.

## Consistência e Publicação Atômica

A ingestão utiliza um processo de **staging → commit**.

O fluxo é:

```text
1. Consultar todas as séries
        ↓
2. Criar o batch em _staging
        ↓
3. Validar o processamento
        ↓
4. Publicar os objetos na Bronze
        ↓
5. Criar o commit marker
        ↓
6. Atualizar o estado das séries
```

Caso qualquer série apresente erro:

```text
Falha na ingestão
        ↓
Batch não é publicado
        ↓
Commit marker não é criado
        ↓
Estado não é atualizado
        ↓
Staging é removido
```

Assim, uma falha em uma única série não resulta na publicação de um batch parcial.

A camada Silver só processa batches que possuem um **commit marker válido**, evitando que dados incompletos sejam propagados para as etapas seguintes.

## Reprocessamento

A arquitetura também permite reprocessar uma execução que falhou sem depender de um batch parcialmente publicado.

Como o estado só é atualizado após o commit do batch, uma execução com falha não altera a watermark da série.

Isso permite que uma nova execução:

* Reutilize o último estado válido;
* Consulte novamente apenas os dados necessários;
* Reconstrua o snapshot completo;
* Publique o batch somente após todas as séries serem processadas com sucesso.

## Orquestração

O pipeline é executado pelo **Apache Airflow**, seguindo as seguintes etapas:

```text
Ingestion
    ↓
Bronze → Silver
    ↓
Silver → Gold
    ↓
Gold → PostgreSQL
    ↓
dbt
```

Cada execução do Airflow possui um `batch_id`, permitindo rastrear os dados desde a ingestão até as camadas analíticas.

## Infraestrutura

Todo o ambiente é executado utilizando **Docker Compose**, permitindo executar os principais componentes do projeto de forma isolada e reproduzível.

Os principais serviços são:

* Airflow;
* Python / Ingestion;
* MinIO;
* Spark;
* PostgreSQL;
* dbt.

## Objetivo

O projeto tem como objetivo aplicar conceitos de **Engenharia de Dados** em um pipeline completo, incluindo:

* Ingestão de APIs;
* Ingestão incremental;
* Watermark;
* Controle de estado;
* Batches e rastreabilidade;
* Staging e publicação atômica;
* Data Lake;
* Arquitetura Bronze, Silver e Gold;
* Processamento com Spark;
* Particionamento;
* Orquestração com Airflow;
* Data Warehouse;
* Transformações com dbt;
* Controle de duplicidades;
* Reprocessamento seguro;
* Containerização com Docker.
