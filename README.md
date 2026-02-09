# NewsData.io – Pipeline Medallion Completo

Pipeline de dados completo com arquitetura **Medallion** (Bronze → Silver → Gold) e **Dashboard Streamlit**.

## Funcionalidades

- **Bronze Layer** — Ingestão de dados da API NewsData.io
- **Silver Layer** — Limpeza, NLP (sentimento, entidades, língua)
- **Gold Layer** — Agregações e KPIs para análise
- **Dashboard** — Interface Streamlit para executar pipeline e visualizar dados

## Arquitetura Medallion

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA LAKEHOUSE                           │
├─────────────────┬─────────────────┬─────────────────────────────┤
│   🥉 BRONZE     │   🥈 SILVER     │   🥇 GOLD                   │
│   (Raw Data)    │   (Cleaned)     │   (Business Ready)          │
├─────────────────┼─────────────────┼─────────────────────────────┤
│ ✅ JSON da API  │ ✅ Dados limpos │ ✅ Agregações               │
│ ✅ CSV tabular  │ ✅ Sentimento   │ ✅ Daily Summary            │
│ ✅ Parquet      │ ✅ Entidades    │ ✅ Source Stats             │
│ ✅ SQLite DB    │ ✅ Língua       │ ✅ Trending Topics          │
│ ✅ Deduplicação │ ✅ Categorias   │ ✅ Sentiment Timeline       │
└─────────────────┴─────────────────┴─────────────────────────────┘
                              │
                              ▼
                    ┌─────────────────┐
                    │  📊 DASHBOARD   │
                    │   Streamlit     │
                    └─────────────────┘
```

## Estrutura do Projeto

```
newsdata_api/
├── app.py                     ← Dashboard Streamlit
├── main.py                    ← Pipeline CLI
├── src/
│   ├── bronze/
│   │   ├── __init__.py
│   │   ├── ingest.py          ← Ingestão NewsData.io
│   │   └── wiki_scraper.py    ← Web Scraping Wikipedia
│   ├── silver/
│   │   ├── __init__.py
│   │   └── transform.py       ← Transformações NLP
│   ├── gold/
│   │   ├── __init__.py
│   │   └── aggregate.py       ← Agregações e KPIs
│   ├── utils/
│   │   ├── __init__.py
│   │   └── text_processing.py ← Utilitários de texto
│   └── db/
│       ├── __init__.py
│       └── loader.py          ← Carregamento para SQLite
├── collection/
│   └── bronze/                ← Dados coletados
│       ├── newsdata_{endpoint}_raw_{timestamp}.json
│       ├── newsdata_{endpoint}_tabular_{timestamp}.csv
│       └── newsdata_{endpoint}_tabular_{timestamp}.parquet
├── db/
│   └── newsdata.db            ← Base de dados SQLite (gerado)
├── tests/
│   ├── test_bronze.py
│   ├── test_db.py
│   ├── test_silver.py         ← Testes Silver layer
│   └── test_gold.py           ← Testes Gold layer
├── .env                       ← API Key (não commitar!)
├── .gitignore
├── requirements.txt
└── README.md
```

## Instalação

### Windows (D:\)

```cmd
d:
cd newsdata_api

python -m venv venv
venv\Scripts\activate

pip install -r requirements.txt
```

### Linux/Mac

```bash
cd newsdata_api
python3 -m venv venv
source venv/bin/activate

pip install -r requirements.txt
```

## Utilização

### Dashboard Streamlit (Recomendado)

```bash
streamlit run app.py
```

Abre `http://localhost:8501` no browser. O dashboard permite:
- Executar pipeline completo via sidebar
- Selecionar endpoint e tamanho
- Visualizar KPIs, gráficos e tabelas
- Filtrar por fonte e sentimento

### Pipeline CLI

```bash
python main.py
```

Menu interativo para:
1. Escolher endpoint (latestPT, tech, crypto, market)
2. Definir tamanho (1-10 artigos)
3. Processar Silver layer (limpeza + NLP)
4. Calcular Gold layer (agregações)

### Endpoints disponíveis

| Endpoint | Descrição |
|----------|-----------|
| `latestPT` | Últimas notícias de Portugal |
| `tech` | Notícias de tecnologia |
| `crypto` | Notícias de criptomoedas |
| `market` | Notícias de mercados globais |

### Módulos individuais

```bash
# Apenas Bronze (ingestão)
python -m src.bronze.ingest --endpoint tech --size 5

# Carregar CSV existentes para SQLite
python -m src.db.loader
```

## Output

### Bronze Layer (`collection/bronze/`)

| Ficheiro | Descrição |
|----------|-----------|
| `newsdata_{endpoint}_raw_{timestamp}.json` | JSON original da API (raw) |
| `newsdata_{endpoint}_tabular_{timestamp}.csv` | Dados tabulares CSV |
| `newsdata_{endpoint}_tabular_{timestamp}.parquet` | Dados tabulares Parquet |

Artigos duplicados são filtrados automaticamente antes de gravar.

### Base de dados (`db/newsdata.db`)

#### Tabela `artigos` (Bronze)

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `article_id` | TEXT (PK) | Identificador único |
| `title` | TEXT | Título original |
| `description` | TEXT | Resumo |
| `source_id` | TEXT | ID da fonte |
| `pubDate` | TEXT | Data de publicação |
| `category` | TEXT | Categoria(s) |
| `link` | TEXT | URL do artigo |
| `endpoint` | TEXT | Endpoint de origem |

#### Tabela `artigos_silver` (Silver)

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `article_id` | TEXT (PK) | Identificador único |
| `title_clean` | TEXT | Título limpo (sem HTML) |
| `sentiment_polarity` | REAL | Polaridade (-1 a 1) |
| `sentiment_label` | TEXT | positive/negative/neutral |
| `entities_persons` | TEXT | Pessoas detectadas (JSON) |
| `entities_locations` | TEXT | Locais detectados (JSON) |
| `language_detected` | TEXT | Língua detectada (pt/en) |
| `category_primary` | TEXT | Categoria normalizada |
| `pub_date` | TEXT | Data formatada |
| `word_count` | INTEGER | Contagem de palavras |

#### Tabelas Gold (Agregações)

| Tabela | Descrição |
|--------|-----------|
| `gold_daily_summary` | Resumo diário (artigos, sentimento, fontes) |
| `gold_source_stats` | Estatísticas por fonte |
| `gold_trending_topics` | Palavras mais frequentes |
| `gold_sentiment_timeline` | Evolução do sentimento |
| `gold_category_matrix` | Matriz categoria × sentimento |

## Pipeline Completo

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│  NewsData   │    │   BRONZE    │    │   SILVER    │    │    GOLD     │
│    API      │ ─► │  Raw Data   │ ─► │   NLP +     │ ─► │ Agregações  │
│             │    │  + Dedup    │    │  Limpeza    │    │   + KPIs    │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
                                                                │
                                                                ▼
                                                      ┌─────────────────┐
                                                      │   Dashboard     │
                                                      │   Streamlit     │
                                                      └─────────────────┘
```

### Silver Layer — Transformações

- Limpeza de HTML e caracteres especiais
- Análise de sentimento (TextBlob)
- Extração de entidades (pessoas, locais, organizações)
- Detecção de língua (langdetect)
- Normalização de categorias
- Validação de URLs

### Gold Layer — Agregações

- **Daily Summary** — artigos, sentimento médio, fontes por dia
- **Source Stats** — estatísticas por fonte
- **Trending Topics** — palavras mais frequentes
- **Sentiment Timeline** — evolução do sentimento
- **Category Matrix** — distribuição categoria × sentimento

## Testes

```bash
# Todos os testes (132 testes)
python -m pytest tests/ -v

# Apenas Silver
python -m pytest tests/test_silver.py -v

# Apenas Gold
python -m pytest tests/test_gold.py -v
```

## Limites da API (Plano Gratuito)

- 200 pedidos/dia
- Máximo 10 artigos por pedido
- Apenas endpoint `/latest`

## Stack

- **Python 3.10+**
- **pandas** — manipulação de dados
- **TextBlob** — análise de sentimento
- **langdetect** — detecção de língua
- **Streamlit** — dashboard web
- **Plotly** — gráficos interativos
- **SQLite** — armazenamento

## Licença

MIT
