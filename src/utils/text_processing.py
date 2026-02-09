"""
NewsData.io - Processamento de Texto

Funcoes de limpeza e normalizacao de texto para a camada Silver.
"""

import html
import re
import unicodedata


def limpar_html(texto: str | None) -> str | None:
    """
    Remove tags HTML e decodifica entidades HTML.

    Args:
        texto: Texto com possivel HTML

    Returns:
        Texto limpo sem HTML
    """
    if not texto:
        return texto

    # Decodificar entidades HTML (&amp; -> &, &lt; -> <, etc.)
    texto = html.unescape(texto)

    # Remover tags HTML
    texto = re.sub(r"<[^>]+>", " ", texto)

    return texto


def limpar_caracteres_especiais(texto: str | None) -> str | None:
    """
    Remove ou normaliza caracteres especiais.

    Args:
        texto: Texto com caracteres especiais

    Returns:
        Texto normalizado
    """
    if not texto:
        return texto

    # Normalizar unicode (NFD -> NFC)
    texto = unicodedata.normalize("NFC", texto)

    # Remover caracteres de controlo (exceto newlines e tabs)
    texto = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]", "", texto)

    # Substituir varios tipos de aspas por aspas normais
    texto = re.sub(r"[""„]", '"', texto)
    texto = re.sub(r"[''‚]", "'", texto)

    # Substituir travessoes por hifens
    texto = re.sub(r"[–—]", "-", texto)

    # Substituir reticencias unicode por tres pontos
    texto = texto.replace("…", "...")

    return texto


def normalizar_espacos(texto: str | None) -> str | None:
    """
    Remove espacos multiplos e faz trim.

    Args:
        texto: Texto com espacos irregulares

    Returns:
        Texto com espacos normalizados
    """
    if not texto:
        return texto

    # Substituir multiplos espacos/tabs por espaco unico
    texto = re.sub(r"[ \t]+", " ", texto)

    # Substituir multiplas newlines por uma
    texto = re.sub(r"\n{3,}", "\n\n", texto)

    # Trim
    texto = texto.strip()

    return texto


def limpar_texto_completo(texto: str | None) -> str | None:
    """
    Pipeline completo de limpeza de texto.

    Aplica todas as funcoes de limpeza em sequencia:
    1. Remove HTML
    2. Normaliza caracteres especiais
    3. Normaliza espacos

    Args:
        texto: Texto original

    Returns:
        Texto completamente limpo
    """
    if not texto:
        return texto

    texto = limpar_html(texto)
    texto = limpar_caracteres_especiais(texto)
    texto = normalizar_espacos(texto)

    return texto


def extrair_dominio(url: str | None) -> str | None:
    """
    Extrai o dominio de uma URL.

    Args:
        url: URL completa

    Returns:
        Dominio (ex: "publico.pt")
    """
    if not url:
        return None

    # Regex para extrair dominio
    match = re.search(r"https?://(?:www\.)?([^/]+)", url)
    if match:
        return match.group(1)

    return None


def contar_palavras(texto: str | None) -> int:
    """
    Conta o numero de palavras num texto.

    Args:
        texto: Texto a analisar

    Returns:
        Numero de palavras
    """
    if not texto:
        return 0

    # Dividir por espacos e filtrar vazios
    palavras = [p for p in texto.split() if p]
    return len(palavras)
