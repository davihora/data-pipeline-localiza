<img width="400" height="200" alt="image" src="https://github.com/user-attachments/assets/d69c4e3d-091b-41a4-bf42-8302d0667294" />


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
- `make` (pré-instalado no macOS e Linux)
- ~2 GB de espaço em disco

### Execução completa (uma linha)

```bash
cd data-pipeline
make pipeline
```

Este comando executa toda a cadeia em sequência: `setup → build → init → up → run → wait → results`. Ao final, os CSVs de saída são impressos no terminal.

### Execução passo a passo

Execute os comandos abaixo **em ordem** a partir do diretório `data-pipeline/`:

```bash
# 1. Cria os diretórios necessários e o arquivo .env (se ausente)
make setup
```
> Garante que `data/staging`, `data/output`, `logs/` e `plugins/` existam no host e que o `.env` esteja presente. Seguro executar múltiplas vezes.

```bash
# 2. Constrói a imagem Docker customizada do Airflow
make build
```
> Executa `docker compose build`, instalando todas as dependências Python (`requirements.txt`) na imagem. Necessário apenas na primeira vez ou após alterar o `Dockerfile`/`requirements.txt`.

```bash
# 3. Inicializa o banco de metadados do Airflow e cria o usuário admin
make init
```
> Executa `airflow db migrate` e cria o usuário `admin/admin`. Necessário apenas uma vez. O container `airflow-init` encerra sozinho ao terminar.

```bash
# 4. Sobe o webserver e o scheduler em background
make up
```
> Inicia os dois serviços e aguarda o webserver reportar `healthy` antes de retornar. A UI estará disponível em **http://localhost:8080** (usuário: `admin`, senha: `admin`).

```bash
# 5. Dispara o DAG
make run
```
> Envia um trigger manual para o DAG `transactions_pipeline` via CLI do Airflow. O run fica em estado `queued` até o scheduler alocá-lo.

```bash
# 6. Aguarda a conclusão (polling a cada ~5 s)
make wait
```
> Consulta o estado do run mais recente em loop até receber `success` ou `failed`. Pressione `Ctrl-C` para interromper o polling sem cancelar o DAG. Em caso de falha, use `make logs-scheduler` para inspecionar o erro.

```bash
# 7. Exibe os resultados no terminal
make results
```
> Imprime `table1_region_risk.csv` e `table2_top3_receivers.csv` formatados em colunas.

```bash
# 8. Para todos os containers
make down
```
> Executa `docker compose down`. Os dados em `data/output/` e `data/staging/` são preservados.

---

### Referência rápida de targets

| Comando | O que faz |
|---|---|
| `make pipeline` | Atalho completo: `setup → build → init → up → run → wait → results` |
| `make setup` | Cria diretórios e `.env` se ausentes |
| `make build` | Builda a imagem Docker |
| `make init` | Inicializa o banco de metadados do Airflow |
| `make up` | Sobe webserver e scheduler, aguarda healthcheck |
| `make run` | Dispara o DAG uma vez |
| `make wait` | Polling até `success` ou `failed` |
| `make status` | Lista todos os runs do DAG em formato tabular |
| `make results` | Imprime os CSVs de saída no terminal |
| `make down` | Para e remove os containers |
| `make reset` | `down` + apaga staging/output + `airflow.db` |
| `make clean-data` | Remove apenas staging e output (mantém dados brutos) |
| `make test` | Roda os testes unitários dentro do container |
| `make test-cov` | Roda os testes com relatório de cobertura HTML |
| `make logs-scheduler` | Tail dos logs do scheduler (útil para debug) |
| `make logs-webserver` | Tail dos logs do webserver |

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

## Decisões de projeto

**DuckDB para todo o processamento SQL.** O DuckDB roda em processo, sem servidor, lê e escreve Parquet nativamente e processa o dataset completo em memória com execução colunar. Isso elimina a necessidade de um cluster Spark ou banco externo para uma carga de trabalho desta escala (~1 M de linhas).

**Tasks de transformação em paralelo.** `transform_table1` e `transform_table2` não compartilham estado e operam sobre o mesmo arquivo Parquet somente-leitura, portanto rodam de forma concorrente no DAG após a porta de qualidade. `export_results` atua como ponto de junção.

**Porta de qualidade com falha rápida.** A task `dq_check` está posicionada entre a limpeza e a exportação. Se qualquer expectativa crítica falhar, `DataQualityError` é lançado e as tasks downstream nunca são executadas, evitando que dados corrompidos cheguem ao diretório de saída.
