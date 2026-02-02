# NewsData.io – Pipeline Bronze

Exemplo **simples e direto** para ingestão de dados:

1. Definir API Key
2. Fazer request na NewsData.io
3. Salvar JSON (raw)
4. Converter JSON → pandas DataFrame
5. Salvar CSV (bronze tabular)

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
└─────────────────┴─────────────────┴─────────────────────────────┘
```

## Estrutura do Projeto

```
newsdata_api/
├── main.py                    ← Ponto de entrada
├── src/
│   └── bronze/
│       ├── __init__.py
│       └── ingest.py          ← Script de ingestão
├── collection/
│   └── bronze/                ← Dados coletados
│       ├── newsdata_raw_{timestamp}.json
│       └── newsdata_tabular_{timestamp}.csv
├── tests/
├── .env                       ← API Key (não commitar!)
├── .env.example
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

### Executar Pipeline Bronze

```bash
python -m src.bronze.ingest
```

### Opções

```bash
# Notícias de Portugal (default)
python -m src.bronze.ingest

# Notícias de outro país
python -m src.bronze.ingest --country br

# Notícias de uma categoria
python -m src.bronze.ingest --category technology

# Pesquisa por termo
python -m src.bronze.ingest --query "inteligência artificial"

# Mais resultados
python -m src.bronze.ingest --size 20
```

## Output (Bronze Layer)

Após executar, encontras em `collection/bronze/`:

| Ficheiro | Descrição |
|----------|-----------|
| `newsdata_raw_{timestamp}.json` | JSON original da API (raw) |
| `newsdata_tabular_{timestamp}.csv` | Dados tabulares (DataFrame) |

### Exemplo de CSV gerado

| title | description | source_id | pubDate | category | link |
|-------|-------------|-----------|---------|----------|------|
| Notícia 1 | Descrição... | publico | 2024-01-15 | technology | https://... |
| Notícia 2 | Descrição... | observador | 2024-01-15 | business | https://... |

## Pipeline Bronze

```
API → JSON (raw) → DataFrame → CSV
```

1. **Request na API** → Obtém dados brutos
2. **Salva JSON** → Preserva resposta original
3. **Normaliza** → Extrai campos relevantes
4. **Salva CSV** → Pronto para análise

## Limites da API (Plano Bronze)

- 200 pedidos/dia
- Apenas endpoint `/latest`
- Sem acesso a arquivo histórico

## Licença

MIT
