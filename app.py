"""
NewsData.io Dashboard

Aplicacao Streamlit para executar o pipeline e visualizar dados.

Uso:
    streamlit run app.py
"""

import sqlite3
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# Configuracao da pagina
st.set_page_config(
    page_title="NewsData Dashboard",
    page_icon="📰",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Caminho da base de dados
DB_PATH = Path(__file__).parent / "db" / "newsdata.db"

# Endpoints disponiveis
ENDPOINTS = {
    "latestPT": "Ultimas Noticias PT",
    "tech": "Tecnologia",
    "crypto": "Criptomoedas",
    "market": "Mercados Globais",
}


# ============================================================================
# FUNCOES DE BASE DE DADOS
# ============================================================================

@st.cache_resource
def get_connection():
    """Retorna conexao SQLite."""
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def check_db_exists():
    """Verifica se a base de dados existe."""
    return DB_PATH.exists()


@st.cache_data(ttl=60)
def load_kpis():
    """Carrega KPIs principais."""
    if not check_db_exists():
        return None

    conn = get_connection()
    try:
        df = pd.read_sql_query("""
            SELECT
                COUNT(*) as total_artigos,
                COUNT(DISTINCT source_id) as total_fontes,
                AVG(sentiment_polarity) as sentimento_medio,
                SUM(CASE WHEN sentiment_label = 'positive' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as pct_positivo
            FROM artigos_silver
        """, conn)
        return df.iloc[0].to_dict()
    except Exception:
        return None


@st.cache_data(ttl=60)
def load_timeline():
    """Carrega dados de evolucao temporal."""
    if not check_db_exists():
        return pd.DataFrame()

    conn = get_connection()
    try:
        return pd.read_sql_query("""
            SELECT
                pub_date as data,
                AVG(sentiment_polarity) as sentimento,
                COUNT(*) as artigos
            FROM artigos_silver
            WHERE pub_date IS NOT NULL
            GROUP BY pub_date
            ORDER BY pub_date
        """, conn)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=60)
def load_sentiment_distribution():
    """Carrega distribuicao de sentimento."""
    if not check_db_exists():
        return pd.DataFrame()

    conn = get_connection()
    try:
        return pd.read_sql_query("""
            SELECT
                sentiment_label as sentimento,
                COUNT(*) as contagem
            FROM artigos_silver
            WHERE sentiment_label IS NOT NULL
            GROUP BY sentiment_label
        """, conn)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=60)
def load_top_sources(limit=10):
    """Carrega top fontes."""
    if not check_db_exists():
        return pd.DataFrame()

    conn = get_connection()
    try:
        return pd.read_sql_query(f"""
            SELECT
                source_id as fonte,
                COUNT(*) as artigos
            FROM artigos_silver
            GROUP BY source_id
            ORDER BY artigos DESC
            LIMIT {limit}
        """, conn)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=60)
def load_trending_topics(limit=15):
    """Carrega trending topics."""
    if not check_db_exists():
        return pd.DataFrame()

    conn = get_connection()
    try:
        return pd.read_sql_query(f"""
            SELECT
                term as termo,
                frequency as frequencia
            FROM gold_trending_topics
            ORDER BY frequency DESC
            LIMIT {limit}
        """, conn)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=60)
def load_recent_articles(limit=20):
    """Carrega artigos recentes."""
    if not check_db_exists():
        return pd.DataFrame()

    conn = get_connection()
    try:
        return pd.read_sql_query(f"""
            SELECT
                title_clean as titulo,
                source_name as fonte,
                pub_date as data,
                sentiment_label as sentimento,
                category_primary as categoria
            FROM artigos_silver
            ORDER BY pub_date DESC, processed_at DESC
            LIMIT {limit}
        """, conn)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=60)
def load_sources_list():
    """Carrega lista de fontes unicas."""
    if not check_db_exists():
        return []

    conn = get_connection()
    try:
        df = pd.read_sql_query("""
            SELECT DISTINCT source_id FROM artigos_silver ORDER BY source_id
        """, conn)
        return df["source_id"].tolist()
    except Exception:
        return []


def clear_cache():
    """Limpa cache do Streamlit."""
    st.cache_data.clear()


# ============================================================================
# FUNCOES DO PIPELINE
# ============================================================================

def run_pipeline(endpoint: str, size: int, process_silver: bool, process_gold: bool):
    """Executa o pipeline de ingestao."""
    from src.bronze.ingest import (
        carregar_config,
        fetch_news,
        json_to_dataframe,
        salvar_json_raw,
        salvar_csv,
        salvar_parquet,
        filtrar_duplicados,
        ENDPOINTS as EP_CONFIG,
    )
    from src.db.loader import criar_tabela, carregar_csv as db_carregar_csv
    from datetime import datetime, timezone

    project_root = Path(__file__).parent
    output_dir = project_root / "collection" / "bronze"
    output_dir.mkdir(parents=True, exist_ok=True)

    db_dir = project_root / "db"
    db_dir.mkdir(parents=True, exist_ok=True)
    db_path = db_dir / "newsdata.db"

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    # Carregar config
    config = carregar_config()

    # Fetch news
    skip_country = endpoint in ["crypto", "market"]
    country = None if skip_country else "pt"
    language = None if skip_country else "pt"

    data = fetch_news(
        api_key=config["api_key"],
        endpoint=endpoint,
        country=country,
        language=language,
        size=size
    )

    # Salvar JSON raw
    salvar_json_raw(data, output_dir, timestamp, endpoint)

    # Converter para DataFrame
    df = json_to_dataframe(data)

    if df.empty:
        return {"status": "warning", "message": "Nenhum artigo retornado pela API"}

    # Filtrar duplicados
    df_novos = filtrar_duplicados(df, output_dir, endpoint)

    if df_novos.empty:
        return {"status": "info", "message": f"Todos os {len(df)} artigos ja existem"}

    # Salvar CSV e Parquet
    csv_path = salvar_csv(df_novos, output_dir, timestamp, endpoint)
    salvar_parquet(df_novos, output_dir, timestamp, endpoint)

    # Carregar na DB
    conn = sqlite3.connect(db_path)
    criar_tabela(conn)
    db_carregar_csv(conn, csv_path, endpoint)

    result = {"status": "success", "artigos_novos": len(df_novos)}

    # Processar Silver
    if process_silver:
        from src.silver.transform import processar_silver
        n_silver = processar_silver(conn, verbose=False)
        result["silver"] = n_silver

    # Processar Gold
    if process_gold and process_silver:
        from src.gold.aggregate import processar_gold
        gold_stats = processar_gold(conn, verbose=False)
        result["gold"] = sum(gold_stats.values())

    conn.close()

    # Limpar cache
    clear_cache()

    return result


# ============================================================================
# INTERFACE
# ============================================================================

def main():
    """Funcao principal da app."""

    # Titulo
    st.title("📰 NewsData.io Dashboard")

    # ========================================================================
    # SIDEBAR
    # ========================================================================

    with st.sidebar:
        st.header("🚀 Executar Pipeline")

        # Endpoint
        endpoint = st.selectbox(
            "Endpoint",
            options=list(ENDPOINTS.keys()),
            format_func=lambda x: ENDPOINTS[x],
        )

        # Tamanho
        size = st.slider("Artigos a buscar", min_value=1, max_value=10, value=5)

        # Opcoes
        process_silver = st.checkbox("Processar Silver", value=True)
        process_gold = st.checkbox("Calcular Gold", value=True, disabled=not process_silver)

        # Botao executar
        if st.button("🚀 Executar", type="primary", use_container_width=True):
            with st.spinner("A executar pipeline..."):
                try:
                    result = run_pipeline(endpoint, size, process_silver, process_gold)

                    if result["status"] == "success":
                        st.success(f"✅ {result['artigos_novos']} artigos novos!")
                        if "silver" in result:
                            st.info(f"Silver: {result['silver']} processados")
                        if "gold" in result:
                            st.info(f"Gold: {result['gold']} agregacoes")
                    elif result["status"] == "info":
                        st.info(result["message"])
                    else:
                        st.warning(result["message"])

                except Exception as e:
                    st.error(f"Erro: {e}")

        st.divider()

        # Filtros
        st.header("🔍 Filtros")

        sources = load_sources_list()
        selected_sources = st.multiselect(
            "Fontes",
            options=sources,
            default=[],
            placeholder="Todas as fontes"
        )

        selected_sentiment = st.multiselect(
            "Sentimento",
            options=["positive", "negative", "neutral"],
            default=[],
            placeholder="Todos"
        )

        if st.button("🔄 Actualizar Dashboard", use_container_width=True):
            clear_cache()
            st.rerun()

    # ========================================================================
    # AREA PRINCIPAL
    # ========================================================================

    if not check_db_exists():
        st.warning("⚠️ Base de dados nao encontrada. Execute o pipeline primeiro.")
        st.info("👈 Use a sidebar para executar o pipeline e carregar dados.")
        return

    # KPIs
    kpis = load_kpis()

    if kpis is None:
        st.warning("⚠️ Sem dados na camada Silver. Execute o pipeline com 'Processar Silver' activo.")
        return

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("📊 Total Artigos", f"{kpis['total_artigos']:,.0f}")

    with col2:
        st.metric("📰 Fontes", f"{kpis['total_fontes']:,.0f}")

    with col3:
        sentiment_value = kpis['sentimento_medio'] or 0
        sentiment_emoji = "😊" if sentiment_value > 0.1 else "😐" if sentiment_value > -0.1 else "😞"
        st.metric(f"{sentiment_emoji} Sentimento", f"{sentiment_value:.2f}")

    with col4:
        pct_pos = kpis['pct_positivo'] or 0
        st.metric("👍 % Positivo", f"{pct_pos:.1f}%")

    st.divider()

    # Grafico 1: Timeline
    st.subheader("📈 Evolucao Temporal")

    df_timeline = load_timeline()

    if not df_timeline.empty:
        fig = go.Figure()

        fig.add_trace(go.Scatter(
            x=df_timeline["data"],
            y=df_timeline["sentimento"],
            name="Sentimento",
            line=dict(color="#1f77b4", width=2),
            yaxis="y"
        ))

        fig.add_trace(go.Bar(
            x=df_timeline["data"],
            y=df_timeline["artigos"],
            name="Artigos",
            marker_color="#90EE90",
            opacity=0.5,
            yaxis="y2"
        ))

        fig.update_layout(
            yaxis=dict(title="Sentimento", side="left", range=[-1, 1]),
            yaxis2=dict(title="Artigos", side="right", overlaying="y"),
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
            margin=dict(t=30, b=30),
            height=300,
        )

        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Sem dados temporais disponiveis")

    # Graficos 2 e 3: Sentimento + Fontes
    col_sent, col_sources = st.columns(2)

    with col_sent:
        st.subheader("🥧 Distribuicao Sentimento")

        df_sentiment = load_sentiment_distribution()

        if not df_sentiment.empty:
            colors = {"positive": "#2ecc71", "negative": "#e74c3c", "neutral": "#95a5a6"}
            df_sentiment["cor"] = df_sentiment["sentimento"].map(colors)

            fig = px.pie(
                df_sentiment,
                values="contagem",
                names="sentimento",
                color="sentimento",
                color_discrete_map=colors,
                hole=0.4,
            )
            fig.update_layout(margin=dict(t=20, b=20), height=300)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Sem dados de sentimento")

    with col_sources:
        st.subheader("📊 Top Fontes")

        df_sources = load_top_sources()

        if not df_sources.empty:
            fig = px.bar(
                df_sources,
                x="artigos",
                y="fonte",
                orientation="h",
                color="artigos",
                color_continuous_scale="Blues",
            )
            fig.update_layout(
                margin=dict(t=20, b=20),
                height=300,
                showlegend=False,
                yaxis=dict(categoryorder="total ascending"),
                coloraxis_showscale=False,
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Sem dados de fontes")

    # Grafico 4: Trending Topics
    st.subheader("🔥 Trending Topics")

    df_trending = load_trending_topics()

    if not df_trending.empty:
        fig = px.bar(
            df_trending,
            x="frequencia",
            y="termo",
            orientation="h",
            color="frequencia",
            color_continuous_scale="Oranges",
        )
        fig.update_layout(
            margin=dict(t=20, b=20),
            height=400,
            yaxis=dict(categoryorder="total ascending"),
            coloraxis_showscale=False,
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Sem trending topics. Execute o pipeline com 'Calcular Gold' activo.")

    # Tabela: Artigos recentes
    st.subheader("📋 Artigos Recentes")

    df_articles = load_recent_articles()

    if not df_articles.empty:
        # Aplicar filtros se selecionados
        if selected_sources:
            # Filtrar por fonte (se a coluna existir com nome diferente)
            pass  # A query ja retorna source_name, nao source_id

        if selected_sentiment:
            df_articles = df_articles[df_articles["sentimento"].isin(selected_sentiment)]

        # Formatar sentimento com cores
        def color_sentiment(val):
            if val == "positive":
                return "background-color: #d4edda"
            elif val == "negative":
                return "background-color: #f8d7da"
            return "background-color: #e2e3e5"

        st.dataframe(
            df_articles.style.applymap(color_sentiment, subset=["sentimento"]),
            use_container_width=True,
            height=400,
        )
    else:
        st.info("Sem artigos disponiveis")


if __name__ == "__main__":
    main()
