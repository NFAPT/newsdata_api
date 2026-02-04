# NewsData.io – Pipeline Bronze + SQLite

Exemplo **simples e direto** para ingestão de dados:

1. Definir API Key
2. Fazer request na NewsData.io
3. Salvar JSON (raw)
4. Converter JSON → pandas DataFrame
5. Salvar CSV (bronze tabular)
6. Carregar para base de dados SQLite (opcional)

## Arquitetura Medallion

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA LAKEHOUSE                           │
├─────────────────┬─────────────────┬─────────────────────────────┤
│   🥉 BRONZE     │   🥈 SILVER     │   🥇 GOLD                   │
│   (Raw Data)    │   (Cleaned)     │   (Business Ready)          │
├─────────────────┼─────────────────┼─────────────────────────────┤
│ ✅ Este projeto │ • Dados limpos  │ • Agregações                │
│ • JSON da API   │ • Validados     │ • KPIs                      │
│ • CSV tabular   │ • Tipados       │ • Prontos para análise      │
│ • SQLite DB     │                 │                             │
└─────────────────┴─────────────────┴─────────────────────────────┘
```

## Estrutura do Projeto

```
newsdata_api/
├── main.py                    ← Ponto de entrada
├── src/
│   ├── bronze/
│   │   ├── __init__.py
│   │   └── ingest.py          ← Script de ingestão
│   └── db/
│       ├── __init__.py
│       └── loader.py           ← Carregamento para SQLite
├── collection/
│   └── bronze/                ← Dados coletados
│       ├── newsdata_{endpoint}_raw_{timestamp}.json
│       └── newsdata_{endpoint}_tabular_{timestamp}.csv
├── db/
│   └── newsdata.db            ← Base de dados SQLite (gerado)
├── tests/
│   ├── test_bronze.py
│   └── test_db.py
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

### Pipeline Bronze (ingestão)

```bash
python -m src.bronze.ingest
```

No final da execução, é perguntado se queres carregar os dados na base de dados SQLite.

### Endpoints disponíveis

```bash
# Notícias de Portugal (default)
python -m src.bronze.ingest

# Tech news
python -m src.bronze.ingest --endpoint tech

# Crypto news
python -m src.bronze.ingest --endpoint crypto

# Global market news
python -m src.bronze.ingest --endpoint market
```

### Opções adicionais

```bash
# Notícias de outro país
python -m src.bronze.ingest --country br

# Notícias de uma categoria
python -m src.bronze.ingest --category technology

# Pesquisa por termo
python -m src.bronze.ingest --query "inteligência artificial"

# Mais resultados
python -m src.bronze.ingest --size 20
```

### Carregar para SQLite (standalone)

Carrega todos os CSV existentes na base de dados:

```bash
python -m src.db.loader
```

Com caminho personalizado:

```bash
python -m src.db.loader --db-path outro_caminho/dados.db
```

## Output

### Bronze Layer (`collection/bronze/`)

| Ficheiro | Descrição |
|----------|-----------|
| `newsdata_{endpoint}_raw_{timestamp}.json` | JSON original da API (raw) |
| `newsdata_{endpoint}_tabular_{timestamp}.csv` | Dados tabulares (DataFrame) |

### Base de dados (`db/newsdata.db`)

Tabela `artigos` com as seguintes colunas:

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `article_id` | TEXT (PK) | Identificador único do artigo |
| `title` | TEXT | Título |
| `description` | TEXT | Resumo |
| `content` | TEXT | Conteúdo (limitado no plano gratuito) |
| `source_id` | TEXT | Identificador da fonte |
| `source_name` | TEXT | Nome da fonte |
| `source_url` | TEXT | URL da fonte |
| `creator` | TEXT | Autor(es) |
| `pubDate` | TEXT | Data de publicação |
| `category` | TEXT | Categoria(s) |
| `country` | TEXT | País |
| `language` | TEXT | Idioma |
| `link` | TEXT | URL do artigo |
| `image_url` | TEXT | URL da imagem |
| `endpoint` | TEXT | Endpoint de origem (latestPT, crypto, etc.) |
| `loaded_at` | TEXT | Timestamp de carregamento |

Duplicados são ignorados automaticamente (`INSERT OR IGNORE` por `article_id`).

## Pipeline

```
API → JSON (raw) → DataFrame → CSV → SQLite (opcional)
```

1. **Request na API** → Obtém dados brutos
2. **Salva JSON** → Preserva resposta original
3. **Normaliza** → Extrai campos relevantes
4. **Salva CSV** → Dados tabulares
5. **Carrega SQLite** → Base de dados consultável

## Testes

```bash
python -m pytest tests/ -v
```

## Limites da API (Plano Bronze)

- 200 pedidos/dia
- Apenas endpoint `/latest`
- Sem acesso a arquivo histórico

## Licença

MIT
