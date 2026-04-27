# Blockchain Transactions Pipeline

Pipeline de dados ponta a ponta para ingestão, limpeza, validação e agregação de transações blockchain — orquestrado com Apache Airflow e containerizado via Docker Compose.

---

## Visão geral

Os dados de origem chegam em arquivos CSV particionados. O pipeline os processa na etapa de ingestão e produz duas tabelas analíticas como saída final.

### Resultados gerados

| Tabela | Descrição |
|---|---|
| `table1_region_risk.csv` | Regiões ordenadas pela média de `risk_score` (decrescente) |
| `table2_top3_receivers.csv` | Top 3 endereços receptores por valor, considerando a transação de `sale` mais recente de cada endereço |

Resultados após o processamento de ~1 milhão de registros limpos:

**Tabela 1 — Regiões por média de risk score**

| location_region | avg_risk_score | transaction_count |
|---|---|---|
| North America | 45.163 | 207 879 |
| South America | 45.135 | 205 562 |
| Asia | 44.989 | 206 431 |
| Africa | 44.910 | 204 158 |
| Europe | 44.601 | 207 514 |

**Tabela 2 — Top 3 endereços receptores (última venda)**

| receiving_address | amount | timestamp |
|---|---|---|
| 0xfe2650…ecf4b | 76 757.0 | 2024-01-02 06:44:13 UTC |
| 0xe37126…235d | 76 716.0 | 2023-12-29 11:17:48 UTC |
| 0x646142…e109 | 76 667.0 | 2024-01-01 16:14:14 UTC |

---

## Arquitetura

```
data-pipeline/
├── dags/
│   └── transactions_pipeline.py   # Definição do DAG Airflow
├── src/
│   ├── ingestion/reader.py        # Leitura das partições e escrita em Parquet
│   ├── cleaning/cleaner.py        # Limpeza dos dados via DuckDB
│   ├── quality/dq_suite.py        # Great Expectations + métricas customizadas
│   └── transforms/aggregations.py # Agregações de negócio
├── data/
│   ├── raw/                       # Partições de entrada (CSV)
│   ├── staging/                   # Arquivos Parquet intermediários
│   └── output/                    # Saídas finais em CSV e Parquet
├── great_expectations/gx/         # Contexto GX, expectations e Data Docs
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
```

### Topologia do DAG

```
validate_raw_input
       │
     ingest
       │
     clean
       │
  ┌────┴──────────────────────┐
dq_check   transform_table1   transform_table2
  └────┬──────────────────────┘
       │
  export_results
```

---

## Etapas do pipeline

### 1 · `validate_raw_input`
Verificação inicial da existência e integridade dos arquivos de entrada antes de qualquer processamento. O resumo da validação é enviado ao XCom.

### 2 · `ingest`
Lê as partições CSV, consolida o conteúdo em memória e realiza o parse com o leitor CSV do PyArrow. O resultado é gravado em `staging/transactions_raw.parquet` (compressão Snappy).

### 3 · `clean`
Executa um pipeline SQL multi-etapa no DuckDB sobre o arquivo Parquet bruto:

1. Conversão de tipos: `timestamp` (inteiro Unix epoch → `TIMESTAMP`), `amount` e `risk_score` para numérico
2. Normalização de endereços Ethereum para letras minúsculas
3. Remoção de linhas duplicadas exatas (`SELECT DISTINCT`)
4. Descarte de linhas com `NULL` em colunas críticas (`timestamp`, `sending_address`, `receiving_address`, `amount`, `transaction_type`, `location_region`, `risk_score`, `anomaly`)
5. Descarte de linhas com `amount <= 0`
6. Filtro de valores inválidos em `location_region`

As métricas de limpeza (contagens antes/depois de cada etapa) são enviadas ao XCom.

### 4 · `dq_check`
Executa duas verificações de qualidade independentes sobre o Parquet limpo:

- **Suite Great Expectations** — valida contagem de linhas, restrições de não-nulo nas colunas críticas, valores categóricos permitidos (`transaction_type`, `anomaly`, `age_group`, `purchase_pattern`) e intervalos numéricos de `risk_score`, `amount`, `login_frequency` e `session_duration`. Gera Data Docs em HTML.
- **Métricas customizadas via DuckDB** — calcula contagem de nulos por coluna, endereços Ethereum inválidos, `risk_score` fora do intervalo, percentual de conformidade e distribuição de anomalias. Lança `DataQualityError` se alguma expectativa crítica falhar (configurável via `raise_on_failure`).

### 5 · `transform_table1` / `transform_table2`
Ambas as tarefas rodam em paralelo após a etapa de limpeza:

- **Tabela 1**: `GROUP BY location_region`, `AVG(risk_score)` ordenado de forma decrescente.
- **Tabela 2**: Função de janela (`ROW_NUMBER OVER PARTITION BY receiving_address ORDER BY timestamp DESC`) para isolar a transação de `sale` mais recente por endereço, retornando os 3 maiores por valor.

### 6 · `export_results`
Lê ambos os outputs Parquet com DuckDB e os escreve como CSV em `data/output/`. O conteúdo completo das tabelas é registrado no log da task para inspeção imediata.

---

## Stack tecnológica

| Componente | Versão | Função |
|---|---|---|
| Apache Airflow | 2.9.3 | Orquestração |
| DuckDB | 0.10.3 | Engine SQL em processo |
| PyArrow | 16.1.0 | I/O Parquet e parsing CSV |
| Great Expectations | 0.18.19 | Qualidade de dados declarativa |
| pandas | 2.2.2 | Interface com o validador GX |
| Python | 3.11 | Runtime |
| Docker / Compose | — | Containerização |

**Decisões de infraestrutura:**  
O SQLite é usado como banco de metadados do Airflow (sem necessidade de um serviço Postgres separado) e o `SequentialExecutor` é configurado — adequado para um ambiente de máquina única onde o paralelismo é gerenciado no nível do DAG.

---

## Executando localmente

### Pré-requisitos

- Docker Desktop (ou Colima no macOS)
- `make` (pré-instalado no macOS/Linux)
- ~2 GB de espaço em disco

### Execução com Make (recomendado)

O projeto inclui um Makefile que orquestra todo o ciclo de vida do pipeline. Liste os alvos disponíveis com:

```bash
cd data-pipeline
make help
```

#### Executar tudo de uma vez

```bash
make pipeline
```

Equivale a executar em sequência: `setup` → `build` → `init` → `up` → `run` → `wait` → `results`. Útil para reproduzir o pipeline do zero.

#### Passo a passo

```bash
# Criar diretórios e .env (se não existirem)
make setup

# Construir a imagem Docker customizada
make build

# Inicializar o banco de metadados do Airflow e criar o usuário admin
make init

# Subir webserver e scheduler em background (aguarda o healthcheck)
make up

# Disparar o DAG
make run

# Acompanhar a execução até a conclusão (polling a cada 5 s)
make wait

# Imprimir os CSVs de saída no terminal
make results
```

#### Referência de alvos

| Alvo | Descrição |
|---|---|
| `make pipeline` | Ciclo completo: setup → build → init → up → run → wait → results |
| `make setup` | Cria diretórios e `.env` a partir do `.env.example` |
| `make build` | Constrói a imagem Docker |
| `make init` | Inicializa o banco Airflow e cria o usuário admin |
| `make up` | Sobe webserver + scheduler (aguarda healthcheck) |
| `make run` | Dispara o DAG `transactions_pipeline` |
| `make wait` | Polling até o run terminar (sucesso ou falha) |
| `make status` | Lista todos os runs do DAG em formato tabular |
| `make results` | Imprime os CSVs de saída formatados no terminal |
| `make logs-scheduler` | Tail dos logs do container scheduler |
| `make logs-webserver` | Tail dos logs do container webserver |
| `make clean-data` | Remove artefatos de staging e output (mantém dados raw) |
| `make reset` | Para containers + apaga todos os dados gerados |
| `make down` | Para todos os containers |

O webserver estará disponível em **http://localhost:8080** (credenciais: `admin` / `admin`).

### Configuração manual (alternativa)

```bash
# 1. Entre no diretório do projeto
cd data-pipeline

# 2. Inicialize o banco de metadados do Airflow e crie o usuário admin
docker compose up airflow-init

# 3. Suba o webserver e o scheduler
docker compose up -d airflow-webserver airflow-scheduler
```

#### Disparar o pipeline manualmente

Pela UI: habilite o DAG `transactions_pipeline` e clique em **Trigger DAG**.

Pela CLI:
```bash
docker compose exec airflow-webserver \
  airflow dags trigger transactions_pipeline
```

#### Verificar os resultados

```bash
# Tabela 1
cat data/output/table1_region_risk.csv

# Tabela 2
cat data/output/table2_top3_receivers.csv
```

#### Encerrar

```bash
# Via Make
make down

# Via Docker Compose
docker compose down
```

---

## Variáveis de ambiente

Todos os caminhos são injetados na inicialização do container via `docker-compose.yml` e podem ser sobrescritos no `.env`:

| Variável | Padrão (dentro do container) |
|---|---|
| `PIPELINE_RAW_DIR` | `/opt/airflow/data/raw` |
| `PIPELINE_STAGING_DIR` | `/opt/airflow/data/staging` |
| `PIPELINE_OUTPUT_DIR` | `/opt/airflow/data/output` |
| `PIPELINE_GX_ROOT` | `/opt/airflow/great_expectations` |

---

## Relatório de qualidade de dados

O Great Expectations gera Data Docs HTML a cada execução do pipeline, salvos em:

```
great_expectations/gx/uncommitted/data_docs/local_site/index.html
```

Abra o arquivo no navegador para ver o detalhamento de quais expectations passaram ou falharam.

---

## Testes unitários

Os testes cobrem os quatro módulos `src/` com pytest e fixtures reutilizáveis em `tests/conftest.py`.

### Estrutura

```
tests/
  conftest.py              # fixtures compartilhadas (tmp_raw_dir, make_parquet, clean_parquet)
  unit/
    test_reader.py         # ingestion/reader.py — 10 casos
    test_cleaner.py        # cleaning/cleaner.py — 14 casos
    test_aggregations.py   # transforms/aggregations.py — 11 casos
    test_dq_suite.py       # quality/dq_suite.py — 14 casos
```

### Executar

```bash
# Dentro do container (recomendado — mesmas dependências do pipeline)
make test

# Com relatório de cobertura HTML em htmlcov/
make test-cov

# Localmente (requer dependências instaladas)
cd data-pipeline
python -m pytest tests/unit -v
```

### Cobertura por módulo

| Módulo | Casos | O que é verificado |
|---|---|---|
| `ingestion/reader.py` | 10 | Ordenação de partições, diretório vazio, arquivo vazio, variantes de null, multi-partição, colunas faltantes |
| `cleaning/cleaner.py` | 14 | Deduplicação, nulls críticos, amount ≤ 0, normalização de endereços, enums inválidos, region numérica, input vazio |
| `transforms/aggregations.py` | 11 | Agrupamento por região, ordenação desc, limite de 3, latest sale por endereço, sem sales, input vazio |
| `quality/dq_suite.py` | 14 | compliance 100%, null counts, amount inválido, endereço uppercase, risk_score fora do range, anomaly breakdown |

> `run_gx_suite` não é testado unitariamente — tem side-effects de I/O (GX context, Data Docs HTML) e pertence a testes de integração.

---

## Arquitetura sugerida na AWS

A arquitetura separa responsabilidades em três camadas: orquestração (MWAA), processamento (ECS Fargate) e transformação SQL (Athena). O MWAA fica leve — apenas dispara tarefas — enquanto cada etapa intensiva roda em container dedicado com recursos ajustáveis de forma independente.

### Trigger automático por evento S3

O DAG é auto-perpetuante: após cada execução bem-sucedida, `TriggerDagRunOperator` cria imediatamente uma nova run que estaciona em `SqsSensor` aguardando a próxima mensagem. Nenhuma infraestrutura externa é necessária além da fila SQS.

```
S3 bucket/raw/part_N.csv
        │
        │  S3 Event Notification (ObjectCreated, prefix=raw/, suffix=.csv)
        ▼
  ┌───────────┐
  │    SQS    │   fila gerenciada, retenção configurável
  └─────┬─────┘
        │  SqsSensor (mode=reschedule, poke 30 s)
        ▼
  Run N  ──► ingest ──► clean ──► dq_check ──► transforms ──► export
                                                                  │
                                                    TriggerDagRunOperator
                                                                  │
  Run N+1 ──► [SqsSensor aguardando] ◄─────────────────────────────┘
```

`SqsSensor` usa `mode=reschedule` — libera o slot de worker entre pokes, então o DAG não consome recursos enquanto aguarda.

Quando `SQS_QUEUE_URL` não está definida (execução local), as tasks `sqs_listener` e `trigger_next_run` são omitidas automaticamente e o pipeline funciona como antes.

### Diagrama completo

```
  S3 bucket/raw/      SQS             MWAA
  new CSV ──────► ObjectCreated ──► sqs_listener (sensor, reschedule)
                                          │
                   ┌────────────────────────────────────────────────┐
                   │                    AWS                         │
                   │                                                │
                   │  Amazon S3                                     │
                   │  ┌────────────────────────────────────────┐    │
                   │  │ /raw/  /staging/          /output/     │    │
                   │  │ *.csv  *_raw.parquet       *.csv        │    │
                   │  │        *_clean.parquet     *.parquet   │    │
                   │  └────────────────────────────────────────┘    │
                   │                                                │
                   │  MWAA (orquestrador — não executa processamento)│
                   │  ┌────────────────────────────────────────┐    │
                   │  │  sqs_listener  (SqsSensor)             │    │
                   │  │       │                                │    │
                   │  │  validate_raw_input                    │    │
                   │  │       │                                │    │
                   │  │  ECSOperator ──► Fargate: ingest        │    │
                   │  │       │                                │    │
                   │  │  ECSOperator ──► Fargate: clean         │    │
                   │  │       │                                │    │
                   │  │  ECSOperator ──► Fargate: dq_check      │    │
                   │  │       │                                │    │
                   │  │  ┌────┴──────────────────┐            │    │
                   │  │  AthenaOp           AthenaOp          │    │
                   │  │  transform_table1   transform_table2  │    │
                   │  │  └────┬──────────────────┘            │    │
                   │  │  AthenaOp: export_results              │    │
                   │  │       │                                │    │
                   │  │  trigger_next_run  (TriggerDagRunOp)  │    │
                   │  └───────┼────────────────────────────────┘    │
                   │          │ nova run → volta ao sqs_listener     │
                   │          ▼                                      │
                   │  ┌────────────┐  ┌──────────┐  ┌───────────┐   │
                   │  │   Athena   │  │QuickSight│  │    ECR    │   │
                   │  └────────────┘  └──────────┘  └───────────┘   │
                   │  ┌────────────┐  ┌────────────────────────┐    │
                   │  │  Secrets   │  │    Amazon CloudWatch   │    │
                   │  │  Manager   │  │  (logs, alarmes)       │    │
                   │  └────────────┘  └────────────────────────┘    │
                   └────────────────────────────────────────────────┘
```

### Mapeamento local → AWS

| Componente local | Serviço AWS | Observação |
|---|---|---|
| `data/raw/`, `data/staging/`, `data/output/` | Amazon S3 | Prefixos `s3://bucket/raw/`, `/staging/`, `/output/` |
| Trigger manual (`make run`) | Amazon SQS + S3 Event Notification | Novo CSV em `raw/` envia mensagem para fila SQS; `SqsSensor` consome |
| Docker Compose + Airflow local | Amazon MWAA | Orquestrador puro; usa `ECSOperator` e `AthenaOperator` |
| Tasks `ingest`, `clean`, `dq_check` | ECS Fargate (via `ECSOperator`) | Container dedicado por task; CPU/RAM configuráveis individualmente |
| Tasks `transform_table1/2`, `export_results` | Amazon Athena (via `AthenaOperator`) | `GROUP BY` e window functions puras; sem servidor; custo por query |
| Imagem Docker customizada | Amazon ECR | Mesma imagem usada pelo MWAA e pelos containers Fargate |
| `airflow.db` (SQLite) | Aurora PostgreSQL (gerenciado pelo MWAA) | Substituição automática, sem ação manual |
| Variáveis em `.env` | AWS Secrets Manager | Fernet key e credenciais injetadas via `SecretsManagerBackend` |
| Logs em `logs/` | Amazon CloudWatch Logs | Integração nativa do MWAA e dos containers Fargate |
| Great Expectations (local) | Great Expectations + S3 Data Docs | Stores e Data Docs configurados para bucket S3 |
| `cat data/output/*.csv` | Amazon Athena / QuickSight | Query direta nos Parquet; dashboards opcionais via QuickSight |

### Mudanças necessárias no código

**1. Substituir `PythonOperator` por `ECSOperator` nas tasks intensivas:**

```python
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator

ingest = EcsRunTaskOperator(
    task_id="ingest",
    cluster="data-pipeline-cluster",
    task_definition="data-pipeline-ingest",
    launch_type="FARGATE",
    overrides={
        "containerOverrides": [{
            "name": "pipeline",
            "command": ["python", "-m", "ingestion.reader"],
            "environment": [
                {"name": "PIPELINE_RAW_DIR",     "value": "s3://bucket/raw/"},
                {"name": "PIPELINE_STAGING_DIR", "value": "s3://bucket/staging/"},
            ],
        }]
    },
    network_configuration={"awsvpcConfiguration": {
        "subnets": ["subnet-xxxx"],
        "securityGroups": ["sg-xxxx"],
        "assignPublicIp": "ENABLED",
    }},
)
```

**2. Substituir `transform_table1/2` e `export_results` por `AthenaOperator`:**

```python
from airflow.providers.amazon.aws.operators.athena import AthenaOperator

transform_t1 = AthenaOperator(
    task_id="transform_table1",
    query="""
        SELECT location_region,
               AVG(risk_score)        AS avg_risk_score,
               COUNT(*)               AS transaction_count
        FROM   "pipeline_db"."transactions_clean"
        GROUP  BY location_region
        ORDER  BY avg_risk_score DESC
    """,
    database="pipeline_db",
    output_location="s3://bucket/output/athena_results/",
)
```

**3. Habilitar `httpfs` no DuckDB nos containers Fargate (tasks `ingest` e `clean`):**

```python
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute("SET s3_region = 'us-east-1';")
```

**4. Configurar stores do Great Expectations para S3:**

```yaml
stores:
  validations_store:
    class_name: ValidationsStore
    store_backend:
      class_name: TupleS3StoreBackend
      bucket: my-pipeline-bucket
      prefix: gx/validations/
data_docs_sites:
  s3_site:
    class_name: SiteBuilder
    store_backend:
      class_name: TupleS3StoreBackend
      bucket: my-pipeline-bucket
      prefix: gx/data_docs/
```

**5. Publicar a imagem no ECR:**

```bash
aws ecr get-login-password | docker login --username AWS --password-stdin <account>.dkr.ecr.<region>.amazonaws.com
docker build -t data-pipeline-airflow .
docker tag data-pipeline-airflow:latest <account>.dkr.ecr.<region>.amazonaws.com/data-pipeline-airflow:latest
docker push <account>.dkr.ecr.<region>.amazonaws.com/data-pipeline-airflow:latest
```

### Por que essa combinação?

**MWAA leve** — não executa processamento; apenas agenda e monitora tarefas, o que reduz o tamanho e o custo do ambiente Airflow.

**ECS Fargate por task** — cada etapa intensiva (`ingest`, `clean`, `dq_check`) tem CPU e memória ajustados de forma independente; o container morre ao terminar, sem custo ocioso.

**Athena para transformações** — `transform_table1/2` são `GROUP BY` e window functions puras sobre Parquet no S3; Athena executa sem servidor e cobra por quantidade de dados escaneados (tipicamente centavos por execução).

**SQS + `SqsSensor`** — o próprio Airflow consome a fila usando `mode=reschedule`, liberando o worker slot entre pokes. Basta configurar o S3 Event Notification para enviar mensagens à fila SQS.

**Auto-perpetuação via `TriggerDagRunOperator`** — elimina cron jobs e garante que sempre há exatamente uma run estacionada na fila, processando cada arquivo na ordem de chegada.

**Secrets Manager** — elimina o `.env` em produção e centraliza a rotação de credenciais sem redeploy.

---

## IaC com Terraform

Toda a infraestrutura AWS é provisionada e versionada via Terraform, organizada em módulos independentes:

```
infra/terraform/
  main.tf            # provider AWS, backend S3 + DynamoDB (state remoto e lock)
  variables.tf       # região, nomes de buckets, config MWAA
  outputs.tf         # ARNs e URLs exportados
  modules/
    s3/              # 3 buckets (raw, staging, output) + versionamento + lifecycle
    sqs/             # fila principal + DLQ + S3 Event Notification (prefix=raw/, suffix=.csv)
    mwaa/            # ambiente MWAA + IAM role + security groups
    ecr/             # repositório de imagem + política de lifecycle
    monitoring/      # CloudWatch Alarms + SNS topic + subscrições (e-mail, Slack, Teams)
```

**Recursos provisionados:**

| Módulo | Recursos |
|---|---|
| `s3` | `aws_s3_bucket` × 3, `aws_s3_bucket_versioning`, `aws_s3_bucket_lifecycle_configuration` |
| `sqs` | `aws_sqs_queue` (principal + DLQ), `aws_s3_bucket_notification` |
| `mwaa` | `aws_mwaa_environment`, `aws_iam_role`, `aws_iam_role_policy`, `aws_security_group` |
| `ecr` | `aws_ecr_repository`, `aws_ecr_lifecycle_policy` |
| `monitoring` | `aws_cloudwatch_metric_alarm` × N, `aws_sns_topic`, `aws_sns_topic_subscription` |

**Inicializar e aplicar:**

```bash
cd infra/terraform
terraform init
terraform plan -var-file=envs/prod.tfvars
terraform apply -var-file=envs/prod.tfvars
```

---

## CI/CD com GitHub Actions

Pipeline de 4 stages em `.github/workflows/pipeline.yml`:

```
push / PR
    │
    ├── [test]         python -m pytest tests/unit --cov=src
    ├── [lint]         ruff check src/ tests/ dags/
    │
    │   (apenas branch main)
    ├── [build-push]   docker build → aws ecr push
    └── [deploy]       aws s3 sync dags/ → s3://bucket/dags/
                       terraform apply (infra changes)
```

```yaml
# .github/workflows/pipeline.yml (estrutura)
on:
  push:
    branches: [main]
  pull_request:

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: pip install -r requirements.txt
      - run: pytest tests/unit --cov=src --cov-fail-under=80

  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: pip install ruff && ruff check src/ tests/ dags/

  build-push:
    needs: [test, lint]
    if: github.ref == 'refs/heads/main'
    steps:
      - uses: aws-actions/configure-aws-credentials@v4
      - uses: aws-actions/amazon-ecr-login@v2
      - run: |
          docker build -t $ECR_REPO:$GITHUB_SHA .
          docker push $ECR_REPO:$GITHUB_SHA

  deploy:
    needs: build-push
    steps:
      - run: aws s3 sync dags/ s3://$MWAA_BUCKET/dags/
      - run: cd infra/terraform && terraform apply -auto-approve
```

---

## Catálogo de dados (AWS Glue Data Catalog)

Um Glue Crawler varre os prefixos S3 e registra as tabelas automaticamente no Catálogo, que o Athena usa como fonte de metadados:

```
Glue Crawler
  ├── s3://bucket/staging/transactions_raw.parquet    → tabela: transactions_raw
  ├── s3://bucket/staging/transactions_clean.parquet  → tabela: transactions_clean
  ├── s3://bucket/output/table1_region_risk.parquet   → tabela: table1_region_risk
  └── s3://bucket/output/table2_top3_receivers.parquet → tabela: table2_top3_receivers
```

O crawler é disparado ao final de cada pipeline via `GlueCrawlerOperator` no DAG (opcional):

```python
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator

refresh_catalog = GlueCrawlerOperator(
    task_id="refresh_glue_catalog",
    config={"Name": "pipeline-crawler"},
)
export >> refresh_catalog >> trigger_next_run
```

Com as tabelas no catálogo, qualquer analista pode consultar os dados direto pelo console Athena sem precisar conhecer os caminhos S3.

---

## Mensageria de falhas

Duas camadas de alertas, com escopos complementares:

**Camada 1 — `on_failure_callback` no DAG** (zero infra extra, cobre falhas de tasks)

```python
import requests

def _notify_failure(context):
    dag_id   = context["dag"].dag_id
    task_id  = context["task_instance"].task_id
    run_id   = context["run_id"]
    log_url  = context["task_instance"].log_url
    msg = f":red_circle: *{dag_id} / {task_id}* falhou\nRun: `{run_id}`\n<{log_url}|Ver log>"
    for url in (os.getenv("SLACK_WEBHOOK_URL"), os.getenv("TEAMS_WEBHOOK_URL")):
        if url:
            requests.post(url, json={"text": msg}, timeout=5)

default_args = {
    ...
    "on_failure_callback": _notify_failure,
}
```

**Camada 2 — CloudWatch Alarms → SNS** (cobre falhas de infra e fila represada)

| Alarme | Métrica | Threshold | Destino |
|---|---|---|---|
| DAG falhou | `MWAA/DAGRunFailed` | ≥ 1 | SNS → e-mail + Slack + Teams |
| Fargate task saiu com erro | `ECS/TaskExitCode ≠ 0` | ≥ 1 | SNS → e-mail |
| DLQ com mensagens | `SQS/ApproximateNumberOfMessagesVisible` DLQ | ≥ 1 | SNS → e-mail + Slack |
| Arquivo não processado há N horas | Metric Math sobre SQS age | configurável | SNS → e-mail |

O SNS topic tem três subscrições: e-mail, endpoint HTTP do Slack webhook e endpoint HTTP do Teams webhook (via `aws_sns_topic_subscription` no Terraform).

---

## Decisões de projeto

**DuckDB para todo o processamento SQL.** O DuckDB roda em processo, sem servidor, lê e escreve Parquet nativamente e processa o dataset completo em memória com execução colunar. Isso elimina a necessidade de um cluster Spark ou banco externo para uma carga de trabalho desta escala (~1 M de linhas).

**Tasks de transformação em paralelo.** `transform_table1` e `transform_table2` não compartilham estado e operam sobre o mesmo arquivo Parquet somente-leitura, portanto rodam de forma concorrente no DAG após a porta de qualidade. `export_results` atua como ponto de junção.

**Porta de qualidade com falha rápida.** A task `dq_check` está posicionada entre a limpeza e a exportação. Se qualquer expectativa crítica falhar, `DataQualityError` é lançado e as tasks downstream nunca são executadas, evitando que dados corrompidos cheguem ao diretório de saída.
