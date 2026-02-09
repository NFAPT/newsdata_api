"""
Testes unitários para a Bronze Layer.

Utiliza mocks para não consumir requests da API durante os testes.
"""

import json
import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import Mock, patch

from src.bronze.ingest import (
    fetch_news,
    json_to_dataframe,
    salvar_json_raw,
    salvar_csv,
    salvar_parquet,
)


# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture
def mock_api_response():
    """Mock de resposta bem-sucedida da API NewsData.io"""
    return {
        "status": "success",
        "totalResults": 2,
        "results": [
            {
                "article_id": "abc123",
                "title": "Notícia de Teste 1",
                "description": "Descrição da notícia 1",
                "content": "Conteúdo completo...",
                "source_id": "publico",
                "source_name": "Público",
                "source_url": "https://publico.pt",
                "creator": ["João Silva"],
                "pubDate": "2024-01-15 10:30:00",
                "category": ["technology", "business"],
                "country": ["portugal"],
                "language": "portuguese",
                "link": "https://publico.pt/noticia1",
                "image_url": "https://publico.pt/img1.jpg",
            },
            {
                "article_id": "def456",
                "title": "Notícia de Teste 2",
                "description": "Descrição da notícia 2",
                "content": "Outro conteúdo...",
                "source_id": "observador",
                "source_name": "Observador",
                "source_url": "https://observador.pt",
                "creator": None,
                "pubDate": "2024-01-15 11:00:00",
                "category": ["sports"],
                "country": ["portugal"],
                "language": "portuguese",
                "link": "https://observador.pt/noticia2",
                "image_url": None,
            },
        ],
    }


@pytest.fixture
def temp_output(tmp_path):
    """Diretório temporário para output."""
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    return output_dir


# ============================================================================
# TESTES DE CONVERSÃO
# ============================================================================

class TestJsonToDataframe:
    """Testes para conversão JSON → DataFrame"""
    
    def test_converte_resultados(self, mock_api_response):
        """Testa conversão de resultados da API"""
        df = json_to_dataframe(mock_api_response)
        
        assert len(df) == 2
        assert "title" in df.columns
        assert "source_id" in df.columns
        assert "pubDate" in df.columns
    
    def test_extrai_campos_corretos(self, mock_api_response):
        """Testa que extrai os campos esperados"""
        df = json_to_dataframe(mock_api_response)
        
        # Verificar primeira linha
        assert df.iloc[0]["title"] == "Notícia de Teste 1"
        assert df.iloc[0]["source_id"] == "publico"
        assert df.iloc[0]["category"] == "technology, business"
    
    def test_trata_listas_como_string(self, mock_api_response):
        """Testa que converte listas para strings"""
        df = json_to_dataframe(mock_api_response)

        # Categorias sao lista -> string
        assert df.iloc[0]["category"] == "technology, business"
        assert df.iloc[1]["category"] == "sports"

        # Creator pode ser None/NaN
        assert df.iloc[0]["creator"] == "João Silva"
        assert pd.isna(df.iloc[1]["creator"])
    
    def test_resultados_vazios(self):
        """Testa resposta sem resultados"""
        data = {"status": "success", "totalResults": 0, "results": []}
        df = json_to_dataframe(data)
        
        assert df.empty


# ============================================================================
# TESTES DE PERSISTÊNCIA
# ============================================================================

class TestSalvarFicheiros:
    """Testes para salvar ficheiros"""

    def test_salvar_json_raw(self, mock_api_response, temp_output):
        """Testa salvar JSON raw"""
        timestamp = "20240115T103000Z"
        endpoint = "latestPT"

        path = salvar_json_raw(mock_api_response, temp_output, timestamp, endpoint)

        assert path.exists()
        assert path.name == f"newsdata_{endpoint}_raw_{timestamp}.json"

        # Verificar conteudo
        with open(path, "r", encoding="utf-8") as f:
            loaded = json.load(f)
        assert loaded["status"] == "success"

    def test_salvar_csv(self, mock_api_response, temp_output):
        """Testa salvar CSV"""
        df = json_to_dataframe(mock_api_response)
        timestamp = "20240115T103000Z"
        endpoint = "latestPT"

        path = salvar_csv(df, temp_output, timestamp, endpoint)

        assert path.exists()
        assert path.name == f"newsdata_{endpoint}_tabular_{timestamp}.csv"

        # Verificar conteudo
        loaded = pd.read_csv(path)
        assert len(loaded) == 2

    def test_salvar_parquet(self, mock_api_response, temp_output):
        """Testa salvar Parquet"""
        df = json_to_dataframe(mock_api_response)
        timestamp = "20240115T103000Z"
        endpoint = "latestPT"

        path = salvar_parquet(df, temp_output, timestamp, endpoint)

        assert path.exists()
        assert path.name == f"newsdata_{endpoint}_tabular_{timestamp}.parquet"

        # Verificar conteudo
        loaded = pd.read_parquet(path)
        assert len(loaded) == 2


# ============================================================================
# TESTES DE API (COM MOCK)
# ============================================================================

class TestFetchNews:
    """Testes para fetch_news com mock"""

    @patch("src.bronze.ingest.requests.get")
    def test_fetch_sucesso(self, mock_get, mock_api_response):
        """Testa request bem-sucedido"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = mock_api_response
        mock_get.return_value = mock_response

        result = fetch_news(
            api_key="test_key",
            endpoint="latestPT",
            country="pt"
        )

        assert result["status"] == "success"
        assert len(result["results"]) == 2

    @patch("src.bronze.ingest.requests.get")
    def test_fetch_erro(self, mock_get):
        """Testa tratamento de erro da API"""
        mock_response = Mock()
        mock_response.status_code = 401
        mock_response.text = "Invalid API key"
        mock_get.return_value = mock_response

        with pytest.raises(Exception, match="Erro na API"):
            fetch_news(
                api_key="invalid_key",
                endpoint="latestPT"
            )

    @patch("src.bronze.ingest.requests.get")
    def test_fetch_diferentes_endpoints(self, mock_get, mock_api_response):
        """Testa diferentes endpoints"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = mock_api_response
        mock_get.return_value = mock_response

        for endpoint in ["latestPT", "tech", "crypto", "market"]:
            result = fetch_news(api_key="test_key", endpoint=endpoint)
            assert result["status"] == "success"


# ============================================================================
# TESTES DE INTEGRACAO
# ============================================================================

class TestIntegracao:
    """Testes de integracao do pipeline completo"""

    @patch("src.bronze.ingest.requests.get")
    def test_pipeline_completo(self, mock_get, mock_api_response, temp_output):
        """Testa pipeline: API -> JSON -> DataFrame -> CSV + Parquet"""
        # Setup mock
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = mock_api_response
        mock_get.return_value = mock_response

        timestamp = "20240115T103000Z"
        endpoint = "latestPT"

        # 1. Fetch
        data = fetch_news(
            api_key="test_key",
            endpoint=endpoint
        )

        # 2. Salvar JSON
        json_path = salvar_json_raw(data, temp_output, timestamp, endpoint)

        # 3. Converter
        df = json_to_dataframe(data)

        # 4. Salvar CSV
        csv_path = salvar_csv(df, temp_output, timestamp, endpoint)

        # 5. Salvar Parquet
        parquet_path = salvar_parquet(df, temp_output, timestamp, endpoint)

        # Verificacoes
        assert json_path.exists()
        assert csv_path.exists()
        assert parquet_path.exists()
        assert len(df) == 2

        # Verificar que os dados sao consistentes
        csv_loaded = pd.read_csv(csv_path)
        parquet_loaded = pd.read_parquet(parquet_path)
        assert len(csv_loaded) == len(parquet_loaded) == 2

    @patch("src.bronze.ingest.requests.get")
    def test_pipeline_sem_resultados(self, mock_get, temp_output):
        """Testa pipeline com resposta vazia da API"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "status": "success",
            "totalResults": 0,
            "results": []
        }
        mock_get.return_value = mock_response

        timestamp = "20240115T103000Z"
        endpoint = "latestPT"

        data = fetch_news(api_key="test_key", endpoint=endpoint)
        df = json_to_dataframe(data)

        assert df.empty


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
