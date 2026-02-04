"""
NewsData.io – Pipeline Bronze (Didático)

Exemplo simples e direto:
1. Definir API Key
2. Fazer request na NewsData.io (Latest ou Crypto)
3. Converter JSON → pandas DataFrame
4. Salvar CSV (bronze)

Uso:
    python -m src.bronze.ingest [opções]
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests
import pandas as pd
from dotenv import load_dotenv


# ============================================================================
# ENDPOINTS DISPONÍVEIS
# ============================================================================

ENDPOINTS = {
    "latestPT": {
        "url": "https://newsdata.io/api/1/latest",
        "nome": "Latest News PT",
        "descricao": "Últimas notícias de Portugal",
    },
    "tech": {
        "url": "https://newsdata.io/api/1/latest",
        "nome": "Latest Tech Headlines",
        "descricao": "Últimas notícias de tecnologia",
        "params_extra": {
            "category": "technology",
        },
    },
    "crypto": {
        "url": "https://newsdata.io/api/1/crypto",
        "nome": "Crypto News",
        "descricao": "Notícias de criptomoedas",
        "params_extra": {
            "prioritydomain": "top",
            "domainurl": "coincodex.com",
        },
    },
    "market": {
        "url": "https://newsdata.io/api/1/market",
        "nome": "Global Market Headlines",
        "descricao": "Notícias de mercados globais",
        "params_extra": {
            "q": "market",
            "language": "en",
            "sort": "pubdateasc",
        },
    },
}


# ============================================================================
# 1️⃣ CONFIGURAÇÃO
# ============================================================================

def carregar_config() -> dict:
    """Carrega configurações do ficheiro .env"""
    # Encontrar .env na raiz do projeto
    project_root = Path(__file__).parent.parent.parent
    env_path = project_root / ".env"
    load_dotenv(env_path)
    
    api_key = os.getenv("NEWSDATA_API_KEY", "")
    if not api_key:
        raise ValueError(
            "❌ NEWSDATA_API_KEY não configurada!\n"
            "   Cria um ficheiro .env com: NEWSDATA_API_KEY=your_key"
        )
    
    return {
        "api_key": api_key,
    }


# ============================================================================
# 2️⃣ REQUEST NA API
# ============================================================================

def fetch_news(
    api_key: str,
    endpoint: str = "latest",
    country: str | None = "pt",
    language: str | None = "pt",
    category: str | None = None,
    query: str | None = None,
    size: int = 10
) -> dict:
    """
    Faz request na API NewsData.io
    
    Args:
        api_key: Chave da API
        endpoint: Tipo de endpoint ("latest" ou "crypto")
        country: Código do país (pt, br, us...)
        language: Código do idioma (pt, en, es...)
        category: Categoria opcional (technology, business, sports...)
        query: Termo de pesquisa opcional
        size: Número de resultados (1-50)
    
    Returns:
        JSON da resposta da API
    """
    # Obter configuração do endpoint
    endpoint_config = ENDPOINTS.get(endpoint, ENDPOINTS["latestPT"])
    base_url = endpoint_config["url"]
    
    # Parâmetros base
    params = {
        "apikey": api_key,
        "size": min(max(1, size), 50),
    }
    
    # Adicionar parâmetros específicos do endpoint
    if "params_extra" in endpoint_config:
        params.update(endpoint_config["params_extra"])
    
    # Parâmetros opcionais (apenas se fornecidos)
    if country:
        params["country"] = country
    if language:
        params["language"] = language
    if category:
        params["category"] = category
    if query:
        params["q"] = query
    
    print(f"📡 A fazer request para {base_url}")
    print(f"   Endpoint: {endpoint_config['nome']}")
    print(f"   Parâmetros: {', '.join(f'{k}={v}' for k, v in params.items() if k != 'apikey')}")
    
    response = requests.get(base_url, params=params, timeout=30)
    
    print(f"   Status: {response.status_code}")
    
    if response.status_code != 200:
        raise Exception(f"Erro na API: {response.status_code} - {response.text}")
    
    return response.json()


# ============================================================================
# 3️⃣ SALVAR JSON RAW
# ============================================================================

def salvar_json_raw(data: dict, output_dir: Path, timestamp: str, endpoint: str = "latest") -> Path:
    """
    Salva JSON original da API (raw)
    
    Args:
        data: Dados da API
        output_dir: Pasta de output
        timestamp: Timestamp para o nome do ficheiro
        endpoint: Tipo de endpoint (latest, crypto)
    
    Returns:
        Caminho do ficheiro guardado
    """
    raw_path = output_dir / f"newsdata_{endpoint}_raw_{timestamp}.json"
    
    with open(raw_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    
    print(f"💾 JSON raw guardado: {raw_path}")
    return raw_path


# ============================================================================
# 4️⃣ CONVERTER JSON → DATAFRAME
# ============================================================================

def json_to_dataframe(data: dict) -> pd.DataFrame:
    """
    Converte JSON da API para DataFrame
    
    Extrai campos relevantes de cada notícia:
    - title, description, content
    - source_id, creator
    - pubDate, category
    - link, image_url
    
    Args:
        data: JSON da API
    
    Returns:
        DataFrame com as notícias
    """
    results = data.get("results", [])
    
    if not results:
        print("⚠️  Nenhum resultado encontrado")
        return pd.DataFrame()
    
    rows = []
    
    for item in results:
        # Extrair categorias como string
        categories = item.get("category", [])
        category_str = ", ".join(categories) if categories else None
        
        # Extrair criadores como string
        creators = item.get("creator", [])
        creator_str = ", ".join(creators) if creators else None
        
        rows.append({
            "title": item.get("title"),
            "description": item.get("description"),
            "content": item.get("content"),
            "source_id": item.get("source_id"),
            "source_name": item.get("source_name"),
            "source_url": item.get("source_url"),
            "creator": creator_str,
            "pubDate": item.get("pubDate"),
            "category": category_str,
            "country": ", ".join(item.get("country", [])) if item.get("country") else None,
            "language": item.get("language"),
            "link": item.get("link"),
            "image_url": item.get("image_url"),
            "article_id": item.get("article_id"),
        })
    
    df = pd.DataFrame(rows)
    print(f"📊 DataFrame criado: {len(df)} linhas × {len(df.columns)} colunas")
    
    return df


# ============================================================================
# 5️⃣ SALVAR CSV (BRONZE)
# ============================================================================

def salvar_csv(df: pd.DataFrame, output_dir: Path, timestamp: str, endpoint: str = "latest") -> Path:
    """
    Salva DataFrame como CSV
    
    Args:
        df: DataFrame com os dados
        output_dir: Pasta de output
        timestamp: Timestamp para o nome do ficheiro
        endpoint: Tipo de endpoint (latest, crypto)
    
    Returns:
        Caminho do ficheiro guardado
    """
    csv_path = output_dir / f"newsdata_{endpoint}_tabular_{timestamp}.csv"
    
    df.to_csv(csv_path, index=False, encoding="utf-8")
    
    print(f"💾 CSV tabular guardado: {csv_path}")
    return csv_path


# ============================================================================
# MAIN
# ============================================================================

def parse_args() -> argparse.Namespace:
    """Parse argumentos da linha de comandos"""
    parser = argparse.ArgumentParser(
        description="NewsData.io – Pipeline Bronze",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos:
  python main.py                          # Latest news Portugal
  python main.py --endpoint tech          # Tech news
  python main.py --endpoint crypto        # Crypto news
  python main.py --endpoint market        # Global market news
  python main.py --endpoint latestPT --country br
  python main.py --query "bitcoin" --size 20
        """
    )
    
    parser.add_argument(
        "--endpoint", "-e",
        choices=["latestPT", "tech", "crypto", "market"],
        default="latestPT",
        help="Tipo de conteúdo: latestPT, tech, crypto, market"
    )
    parser.add_argument(
        "--country", "-c",
        default="pt",
        help="Código do país (default: pt). Ignorado para crypto/market."
    )
    parser.add_argument(
        "--language", "-l",
        default="pt",
        help="Código do idioma (default: pt)"
    )
    parser.add_argument(
        "--category", "-cat",
        help="Categoria (technology, business, sports...)"
    )
    parser.add_argument(
        "--query", "-q",
        help="Termo de pesquisa"
    )
    parser.add_argument(
        "--size", "-s",
        type=int,
        default=10,
        help="Número de resultados (1-50, default: 10)"
    )
    
    return parser.parse_args()


def main() -> int:
    """Pipeline Bronze principal"""
    
    print("\n" + "=" * 60)
    print("    🥉 NEWSDATA.IO – PIPELINE BRONZE")
    print("=" * 60 + "\n")
    
    args = parse_args()
    
    try:
        # 1️⃣ Carregar configuração
        print("1️⃣  A carregar configuração...")
        config = carregar_config()
        
        # 2️⃣ Criar pasta collection/bronze
        project_root = Path(__file__).parent.parent.parent
        output_dir = project_root / "collection" / "bronze"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Timestamp no formato do formador
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        print(f"   Timestamp: {timestamp}")
        print(f"   Endpoint: {ENDPOINTS[args.endpoint]['nome']}\n")
        
        # 3️⃣ Request na API
        print("2️⃣  A fazer request na API...")
        
        # Para crypto e market, não enviamos country (usam parâmetros próprios)
        skip_country = args.endpoint in ["crypto", "market"]
        country = None if skip_country else args.country
        language = None if skip_country else args.language
        
        data = fetch_news(
            api_key=config["api_key"],
            endpoint=args.endpoint,
            country=country,
            language=language,
            category=args.category,
            query=args.query,
            size=args.size
        )
        
        # Verificar status
        status = data.get("status")
        total = data.get("totalResults", 0)
        print(f"   Status: {status}")
        print(f"   Total resultados: {total}\n")
        
        # 4️⃣ Salvar JSON raw
        print("3️⃣  A salvar JSON raw...")
        raw_path = salvar_json_raw(data, output_dir, timestamp, args.endpoint)
        
        # 5️⃣ Converter para DataFrame
        print("\n4️⃣  A converter JSON → DataFrame...")
        df = json_to_dataframe(data)
        
        if not df.empty:
            # Mostrar preview
            print("\n   Preview:")
            print(df[["title", "source_id", "pubDate"]].head().to_string(index=False))
        
        # 6️⃣ Salvar CSV
        print("\n5️⃣  A salvar CSV (bronze tabular)...")
        csv_path = salvar_csv(df, output_dir, timestamp, args.endpoint)
        
        # ✅ Conclusão
        print("\n" + "=" * 60)
        print("✅ PIPELINE BRONZE EXECUTADA COM SUCESSO!")
        print("=" * 60)
        print(f"""
   Endpoint: {ENDPOINTS[args.endpoint]['nome']}
   Ficheiros gerados em: {output_dir}
   
   📄 {raw_path.name}
      → JSON original da API (raw)
   
   📊 {csv_path.name}
      → Dados tabulares ({len(df)} registos)

   Pipeline: API → JSON (raw) → DataFrame → CSV
""")

        # Perguntar se quer carregar na DB
        resposta = input("🗄️  Carregar dados na base de dados SQLite? (s/N): ").strip().lower()
        if resposta == "s":
            import sqlite3
            from src.db.loader import criar_tabela, carregar_csv as db_carregar_csv

            db_dir = project_root / "db"
            db_dir.mkdir(parents=True, exist_ok=True)
            db_path = db_dir / "newsdata.db"

            conn = sqlite3.connect(db_path)
            criar_tabela(conn)
            db_carregar_csv(conn, csv_path, args.endpoint)
            conn.close()
            print(f"✅ Dados carregados em {db_path}")

        return 0
        
    except ValueError as e:
        print(f"\n{e}")
        return 1
    except requests.RequestException as e:
        print(f"\n❌ Erro de conexão: {e}")
        return 1
    except Exception as e:
        print(f"\n❌ Erro inesperado: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
