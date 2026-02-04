"""
Wikipédia PT – Web Scraping Bronze (Didático)

Scraping de páginas da Wikipédia portuguesa:
1. Escolher modo (tema, aleatório, URLs manuais)
2. Obter até 10 páginas
3. Extrair título + resumo
4. Salvar JSON raw + CSV (bronze)

Uso:
    python -m src.bronze.wiki_scraper
"""

import json
import sqlite3
import sys
import io
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import unquote, urlparse

import requests
import pandas as pd


# ============================================================================
# CONSTANTES
# ============================================================================

MAX_PAGINAS = 10

IDIOMAS = {
    "pt": {
        "nome": "Portugues",
        "base_url": "https://pt.wikipedia.org",
    },
    "en": {
        "nome": "Ingles",
        "base_url": "https://en.wikipedia.org",
    },
}

HEADERS = {
    "User-Agent": "NewsDataBot/1.0 (educational project; Python/requests)",
}


def obter_urls(idioma: str = "pt") -> tuple[str, str]:
    """
    Retorna as URLs da API e REST para o idioma escolhido.

    Args:
        idioma: Código do idioma (pt ou en)

    Returns:
        Tupla (api_url, rest_url)
    """
    base = IDIOMAS[idioma]["base_url"]
    return f"{base}/w/api.php", f"{base}/api/rest_v1/page/summary"


# ============================================================================
# 1️⃣ OBTER PÁGINAS POR TEMA
# ============================================================================

def pesquisar_por_tema(tema: str, api_url: str, limite: int = MAX_PAGINAS) -> list[str]:
    """
    Pesquisa títulos de páginas na Wikipédia por tema.

    Args:
        tema: Termo de pesquisa
        api_url: URL da API MediaWiki
        limite: Número máximo de resultados (máx 10)

    Returns:
        Lista de títulos de páginas encontradas
    """
    limite = min(limite, MAX_PAGINAS)

    params = {
        "action": "opensearch",
        "search": tema,
        "limit": limite,
        "namespace": 0,
        "format": "json",
    }

    print(f"   A pesquisar '{tema}'...")
    response = requests.get(api_url, params=params, headers=HEADERS, timeout=30)
    response.raise_for_status()

    data = response.json()
    titulos = data[1] if len(data) > 1 else []

    print(f"   Encontrados: {len(titulos)} resultados")
    return titulos[:limite]


# ============================================================================
# 2️⃣ OBTER PÁGINAS ALEATÓRIAS
# ============================================================================

def obter_aleatorias(api_url: str, limite: int = MAX_PAGINAS) -> list[str]:
    """
    Obtém títulos de páginas aleatórias da Wikipédia.

    Args:
        api_url: URL da API MediaWiki
        limite: Número de páginas (máx 10)

    Returns:
        Lista de títulos de páginas aleatórias
    """
    limite = min(limite, MAX_PAGINAS)

    params = {
        "action": "query",
        "list": "random",
        "rnlimit": limite,
        "rnnamespace": 0,
        "format": "json",
    }

    print(f"   A obter {limite} páginas aleatórias...")
    response = requests.get(api_url, params=params, headers=HEADERS, timeout=30)
    response.raise_for_status()

    data = response.json()
    paginas = data.get("query", {}).get("random", [])
    titulos = [p["title"] for p in paginas]

    print(f"   Obtidas: {len(titulos)} páginas")
    return titulos[:limite]


# ============================================================================
# 3️⃣ EXTRAIR URLs MANUAIS
# ============================================================================

def extrair_titulos_de_urls(urls: list[str]) -> list[str]:
    """
    Extrai títulos de páginas a partir de URLs da Wikipédia.

    Args:
        urls: Lista de URLs (ex: https://pt.wikipedia.org/wiki/Python)

    Returns:
        Lista de títulos extraídos
    """
    titulos = []
    for url in urls[:MAX_PAGINAS]:
        parsed = urlparse(url)
        if "/wiki/" in parsed.path:
            titulo = unquote(parsed.path.split("/wiki/")[-1])
            titulo = titulo.replace("_", " ")
            titulos.append(titulo)
        else:
            print(f"   Aviso: URL ignorado (formato inválido): {url}")

    return titulos


# ============================================================================
# 4️⃣ EXTRAIR RESUMO DE UMA PÁGINA
# ============================================================================

def extrair_resumo(titulo: str, rest_url: str) -> dict | None:
    """
    Extrai título e resumo de uma página da Wikipédia via REST API.

    Args:
        titulo: Título da página
        rest_url: URL base da REST API

    Returns:
        Dicionário com titulo, resumo, url, pageid ou None se falhar
    """
    titulo_url = titulo.replace(" ", "_")
    url = f"{rest_url}/{titulo_url}"

    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        data = response.json()

        return {
            "titulo": data.get("title", titulo),
            "resumo": data.get("extract", ""),
            "url": data.get("content_urls", {}).get("desktop", {}).get("page", ""),
            "pageid": data.get("pageid", ""),
        }

    except requests.RequestException as e:
        print(f"   Erro ao extrair '{titulo}': {e}")
        return None


# ============================================================================
# 5️⃣ SCRAPING COMPLETO
# ============================================================================

def scrape_paginas(titulos: list[str], rest_url: str) -> list[dict]:
    """
    Faz scraping de uma lista de páginas da Wikipédia.

    Args:
        titulos: Lista de títulos (máx 10)
        rest_url: URL base da REST API

    Returns:
        Lista de dicionários com dados extraídos
    """
    titulos = titulos[:MAX_PAGINAS]
    resultados = []
    timestamp = datetime.now(timezone.utc).isoformat()

    for i, titulo in enumerate(titulos, 1):
        print(f"   [{i}/{len(titulos)}] A extrair: {titulo}")
        dados = extrair_resumo(titulo, rest_url)
        if dados:
            dados["timestamp_scrape"] = timestamp
            resultados.append(dados)

    print(f"   Total extraído: {len(resultados)} páginas")
    return resultados


# ============================================================================
# 6️⃣ SALVAR JSON RAW
# ============================================================================

def salvar_json_raw(data: list[dict], output_dir: Path, timestamp: str) -> Path:
    """
    Salva dados raw em JSON.

    Args:
        data: Lista de dados extraídos
        output_dir: Pasta de output
        timestamp: Timestamp para o nome do ficheiro

    Returns:
        Caminho do ficheiro guardado
    """
    raw_path = output_dir / f"wiki_scrape_raw_{timestamp}.json"

    with open(raw_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"   JSON raw guardado: {raw_path}")
    return raw_path


# ============================================================================
# 7️⃣ CONVERTER PARA DATAFRAME + SALVAR CSV
# ============================================================================

def resultados_to_dataframe(resultados: list[dict]) -> pd.DataFrame:
    """
    Converte lista de resultados para DataFrame.

    Args:
        resultados: Lista de dicionários com dados extraídos

    Returns:
        DataFrame com as páginas
    """
    if not resultados:
        print("   Nenhum resultado para converter")
        return pd.DataFrame()

    df = pd.DataFrame(resultados)
    print(f"   DataFrame criado: {len(df)} linhas x {len(df.columns)} colunas")
    return df


def salvar_csv(df: pd.DataFrame, output_dir: Path, timestamp: str) -> Path:
    """
    Salva DataFrame como CSV.

    Args:
        df: DataFrame com os dados
        output_dir: Pasta de output
        timestamp: Timestamp para o nome do ficheiro

    Returns:
        Caminho do ficheiro guardado
    """
    csv_path = output_dir / f"wiki_scrape_tabular_{timestamp}.csv"

    df.to_csv(csv_path, index=False, encoding="utf-8")

    print(f"   CSV tabular guardado: {csv_path}")
    return csv_path


# ============================================================================
# 8️⃣ BASE DE DADOS SQLITE
# ============================================================================

SQL_CRIAR_TABELA_WIKI = """
CREATE TABLE IF NOT EXISTS paginas (
    pageid TEXT PRIMARY KEY,
    titulo TEXT,
    resumo TEXT,
    url TEXT,
    modo TEXT,
    timestamp_scrape TEXT,
    loaded_at TEXT
);
"""


def criar_tabela_wiki(conn: sqlite3.Connection) -> None:
    """Cria a tabela paginas se não existir."""
    conn.execute(SQL_CRIAR_TABELA_WIKI)
    conn.commit()


def carregar_na_db(conn: sqlite3.Connection, resultados: list[dict], modo: str) -> int:
    """
    Insere resultados do scraping na tabela paginas.

    Usa INSERT OR IGNORE para evitar duplicados (pelo pageid).

    Args:
        conn: Conexão SQLite
        resultados: Lista de dicionários com dados extraídos
        modo: Modo de scraping (tema, aleatorio, urls)

    Returns:
        Número de registos inseridos
    """
    loaded_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    registos = []
    for r in resultados:
        registos.append((
            str(r.get("pageid", "")),
            r.get("titulo", ""),
            r.get("resumo", ""),
            r.get("url", ""),
            modo,
            r.get("timestamp_scrape", ""),
            loaded_at,
        ))

    sql = "INSERT OR IGNORE INTO paginas (pageid, titulo, resumo, url, modo, timestamp_scrape, loaded_at) VALUES (?, ?, ?, ?, ?, ?, ?)"
    cursor = conn.executemany(sql, registos)
    conn.commit()

    return cursor.rowcount


# ============================================================================
# MENU INTERATIVO
# ============================================================================

def escolher_idioma() -> str:
    """
    Apresenta menu para escolher o idioma da Wikipédia.

    Returns:
        Código do idioma (pt ou en)
    """
    print("   Escolhe o idioma da Wikipedia:\n")
    print("   [1] Portugues (pt.wikipedia.org)")
    print("   [2] Ingles (en.wikipedia.org)")
    print()

    opcao = input("   Opção (1/2): ").strip()

    if opcao == "2":
        return "en"
    return "pt"


def escolher_modo(api_url: str) -> tuple[str, list[str]]:
    """
    Apresenta menu interativo para escolher o modo de scraping.

    Args:
        api_url: URL da API MediaWiki

    Returns:
        Tupla (modo, titulos)
    """
    print("   Escolhe o modo de scraping:\n")
    print("   [1] Pesquisar por tema")
    print("   [2] Páginas aleatórias")
    print("   [3] Introduzir URLs manualmente")
    print()

    opcao = input("   Opção (1/2/3): ").strip()

    if opcao == "1":
        tema = input("   Tema de pesquisa: ").strip()
        if not tema:
            raise ValueError("Tema não pode ser vazio")
        titulos = pesquisar_por_tema(tema, api_url)
        return "tema", titulos

    elif opcao == "2":
        titulos = obter_aleatorias(api_url)
        return "aleatorio", titulos

    elif opcao == "3":
        print(f"   Introduz até {MAX_PAGINAS} URLs (um por linha, linha vazia para terminar):")
        urls = []
        while len(urls) < MAX_PAGINAS:
            url = input("   URL: ").strip()
            if not url:
                break
            urls.append(url)
        if not urls:
            raise ValueError("Nenhum URL fornecido")
        titulos = extrair_titulos_de_urls(urls)
        return "urls", titulos

    else:
        raise ValueError(f"Opção inválida: {opcao}")


# ============================================================================
# MAIN
# ============================================================================

def main() -> int:
    """Pipeline de scraping da Wikipédia PT"""

    # Garantir UTF-8 no Windows
    if sys.stdout.encoding != "utf-8":
        sys.stdout = io.TextIOWrapper(
            sys.stdout.buffer, encoding="utf-8", errors="replace"
        )

    print("\n" + "=" * 60)
    print("    WIKIPEDIA - WEB SCRAPING BRONZE")
    print("=" * 60 + "\n")

    try:
        # 1️⃣ Escolher idioma
        print("1. A escolher idioma...")
        idioma = escolher_idioma()
        api_url, rest_url = obter_urls(idioma)
        nome_idioma = IDIOMAS[idioma]["nome"]
        print(f"   Wikipedia: {nome_idioma} ({idioma})\n")

        # 2️⃣ Escolher modo
        print("2. A escolher modo de scraping...")
        modo, titulos = escolher_modo(api_url)

        if not titulos:
            print("\n   Nenhuma pagina encontrada.")
            return 1

        print(f"\n   Modo: {modo}")
        print(f"   Paginas a extrair: {len(titulos)}\n")

        # 3️⃣ Criar pasta output
        project_root = Path(__file__).parent.parent.parent
        output_dir = project_root / "collection" / "bronze"
        output_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

        # 4️⃣ Scraping
        print("3. A extrair resumos das paginas...")
        resultados = scrape_paginas(titulos, rest_url)

        if not resultados:
            print("\n   Nenhum resultado extraido.")
            return 1

        # 5️⃣ Salvar JSON raw
        print("\n4. A salvar JSON raw...")
        raw_path = salvar_json_raw(resultados, output_dir, timestamp)

        # 6️⃣ Converter para DataFrame + CSV
        print("\n5. A converter para DataFrame...")
        df = resultados_to_dataframe(resultados)

        if not df.empty:
            print("\n   Preview:")
            print(df[["titulo", "resumo"]].head().to_string(index=False))

        print("\n6. A salvar CSV (bronze tabular)...")
        csv_path = salvar_csv(df, output_dir, timestamp)

        # Conclusao
        print("\n" + "=" * 60)
        print("SCRAPING CONCLUIDO COM SUCESSO!")
        print("=" * 60)
        print(f"""
   Modo: {modo}
   Ficheiros gerados em: {output_dir}

   {raw_path.name}
      -> JSON original (raw)

   {csv_path.name}
      -> Dados tabulares ({len(df)} registos)

   Pipeline: Wikipedia API -> JSON (raw) -> DataFrame -> CSV
""")

        # Perguntar se quer carregar na DB
        resposta = input("   Carregar dados na base de dados SQLite? (s/N): ").strip().lower()
        if resposta == "s":
            db_dir = project_root / "db"
            db_dir.mkdir(parents=True, exist_ok=True)
            db_path = db_dir / "wiki.db"

            conn = sqlite3.connect(db_path)
            criar_tabela_wiki(conn)
            inseridos = carregar_na_db(conn, resultados, modo)
            conn.close()
            print(f"   {inseridos} registos inseridos em {db_path}")

        return 0

    except ValueError as e:
        print(f"\n   Erro: {e}")
        return 1
    except requests.RequestException as e:
        print(f"\n   Erro de conexao: {e}")
        return 1
    except Exception as e:
        print(f"\n   Erro inesperado: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
