# Mini-Projeto - Data App Para Dashboard Interativo de Sales Analytics em Python com Streamlit
# Dataset: Superstore Sales (Tableau) — Varejo americano, 2016-2019


# --- Bloco 1: Importação de Bibliotecas e Configuração da Página ---

import sqlite3
import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st
from fpdf import FPDF
from fpdf.enums import XPos, YPos
from datetime import datetime, date

st.set_page_config(
    page_title="Análise de Varejo Americano - Superstore",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded",
)


# --- Bloco 2: Inicialização com CSV Externo (Superstore Sales) ---

def dsa_init_db(conn):
    """
    Inicializa o banco de dados a partir do CSV do Superstore Sales (Tableau).
    1. Cria a tabela 'tb_vendas' se não existir.
    2. Se estiver vazia, carrega e limpa os dados do CSV.
    3. Insere os dados no banco.

    CORREÇÃO #2: st.error / st.stop removidos (proibido em funções cacheadas).
    Lança FileNotFoundError para ser capturado em datascienceacademy_mp10().
    """

    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS tb_vendas (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            date        TEXT,
            regiao      TEXT,
            categoria   TEXT,
            produto     TEXT,
            faturamento REAL,
            quantidade  INTEGER
        )
    """)
    conn.commit()

    cursor.execute("SELECT COUNT(*) FROM tb_vendas")
    if cursor.fetchone()[0] == 0:

        # -------------------------------------------------------
        # 1. LEITURA DO CSV
        # Coloque o arquivo "Sample - Superstore.csv" na mesma
        # pasta do dsa_app.py antes de rodar.
        # -------------------------------------------------------
        try:
            df_raw = pd.read_csv(
                "Sample - Superstore.csv",
                encoding="latin-1"
            )
        except FileNotFoundError:
            raise FileNotFoundError(
                "Arquivo 'Sample - Superstore.csv' não encontrado. "
                "Coloque-o na mesma pasta do app e recarregue a página."
            )

        # -------------------------------------------------------
        # 2. MAPEAMENTO DE COLUNAS
        # -------------------------------------------------------
        df_raw = df_raw.rename(columns={
            "Order Date"  : "date",
            "Region"      : "regiao",
            "Category"    : "categoria",
            "Product Name": "produto",
            "Sales"       : "faturamento",
            "Quantity"    : "quantidade"
        })

        # -------------------------------------------------------
        # 3. LIMPEZA E PADRONIZAÇÃO
        # -------------------------------------------------------

        # Converte data para formato ISO (YYYY-MM-DD)
        df_raw["date"] = pd.to_datetime(df_raw["date"], dayfirst=False).dt.strftime("%Y-%m-%d")

        # Traduz regiões para Português
        mapa_regioes = {
            "East"   : "Leste",
            "West"   : "Oeste",
            "Central": "Central",
            "South"  : "Sul"
        }
        df_raw["regiao"] = df_raw["regiao"].map(mapa_regioes).fillna(df_raw["regiao"])

        # Traduz categorias para Português
        mapa_categorias = {
            "Furniture"      : "Móveis",
            "Office Supplies": "Escritório",
            "Technology"     : "Tecnologia"
        }
        df_raw["categoria"] = df_raw["categoria"].map(mapa_categorias).fillna(df_raw["categoria"])

        # Trunca nome do produto para 40 caracteres (evita overflow no PDF)
        df_raw["produto"] = df_raw["produto"].str[:40]

        # Garante tipos corretos
        df_raw["faturamento"] = pd.to_numeric(df_raw["faturamento"], errors="coerce").fillna(0).round(2)
        df_raw["quantidade"]  = pd.to_numeric(df_raw["quantidade"],  errors="coerce").fillna(0).astype(int)

        # Seleciona colunas necessárias e remove linhas com nulos
        df_final = df_raw[["date", "regiao", "categoria", "produto", "faturamento", "quantidade"]].dropna()

        # -------------------------------------------------------
        # 4. INSERÇÃO EM MASSA NO SQLITE
        # -------------------------------------------------------
        df_final.to_sql("tb_vendas", conn, if_exists="append", index=False)
        conn.commit()


# --- Bloco 3: Função de Conexão com o Banco de Dados ---
# CORREÇÃO #1: função restaurada (havia sido removida acidentalmente na refatoração)

def dsa_cria_conexao(db_path="dsa_database.db"):
    """
    Cria e retorna conexão SQLite.
    check_same_thread=False é necessário para o Streamlit (multi-thread).
    """
    return sqlite3.connect(db_path, check_same_thread=False)


# --- Bloco 4: Função de Carregamento de Dados com Cache ---

@st.cache_data(ttl=600)
def dsa_carrega_dados():
    """
    Carrega os dados do banco SQLite em um DataFrame Pandas.
    Resultado cacheado por 10 minutos (@st.cache_data).
    Não contém chamadas st.* — CORREÇÃO #2 garante isso.
    """
    conn = dsa_cria_conexao()
    dsa_init_db(conn)
    df = pd.read_sql_query("SELECT * FROM tb_vendas", conn, parse_dates=["date"])
    conn.close()
    return df


# --- Bloco 5: Função da Sidebar e Filtros ---

def dsa_filtros_sidebar(df):
    """
    Cria todos os widgets da sidebar e retorna o DataFrame filtrado.
    """

    st.sidebar.markdown(
        """
        <div style="background-color:#0066CC; padding: 10px; border-radius: 5px; text-align: center; margin-bottom: 15px;">
            <h3 style="color:white; margin:0; font-weight:bold;">🛒 Superstore Sales</h3>
            <p style="color:#CCE5FF; margin:4px 0 0 0; font-size:0.85rem;">Varejo Americano · 2016–2019</p>
        </div>
        """,
        unsafe_allow_html=True
    )

    st.sidebar.header("🔍 Filtros")

    min_date = df["date"].min().date()
    max_date = df["date"].max().date()

    date_range = st.sidebar.date_input(
        "Período de Análise",
        (min_date, max_date),
        min_value=min_date,
        max_value=max_date
    )

    all_regioes    = sorted(df["regiao"].unique())
    all_categorias = sorted(df["categoria"].unique())
    all_produtos   = sorted(df["produto"].unique())

    selected_regioes    = st.sidebar.multiselect("Regiões (EUA)",  all_regioes,    default=all_regioes)
    selected_categorias = st.sidebar.multiselect("Categorias",     all_categorias, default=all_categorias)
    selected_produtos   = st.sidebar.multiselect("Produtos",       all_produtos,   default=all_produtos)

    if len(date_range) == 2:
        start_date, end_date = date_range
    else:
        start_date, end_date = min_date, max_date

    df_dsa_filtrado = df[
        (df["date"].dt.date >= start_date) &
        (df["date"].dt.date <= end_date)   &
        (df["regiao"].isin(selected_regioes))       &
        (df["categoria"].isin(selected_categorias)) &
        (df["produto"].isin(selected_produtos))
    ].copy()

    st.sidebar.markdown("---")

    with st.sidebar.expander("🆘 Suporte / Fale conosco", expanded=False):
        st.write("")

    st.sidebar.caption("Dashboard interativo")

    return df_dsa_filtrado


# --- Bloco 6: Função para Renderizar os Cards de KPIs ---

def dsa_renderiza_cards_kpis(df):
    """
    Calcula e exibe os 4 KPIs principais em cards estilizados.
    Retorna (total_faturamento, total_qty, avg_ticket) para reuso no PDF.
    """

    total_faturamento = df["faturamento"].sum()
    total_qty         = df["quantidade"].sum()
    avg_ticket        = total_faturamento / total_qty if total_qty > 0 else 0
    delta_rev         = np.random.uniform(-5, 15)

    c1, c2, c3, c4 = st.columns(4)

    with c1:
        st.markdown(f"""
        <div class="metric-card">
            <h3>Receita Total (USD)</h3>
            <h2>$ {total_faturamento:,.0f}</h2>
            <div class="delta" style="color: {'#4CAF50' if delta_rev > 0 else '#FF5252'}">
                {delta_rev:+.1f}% vs meta
            </div>
        </div>
        """, unsafe_allow_html=True)

    with c2:
        st.markdown(f"""
        <div class="metric-card">
            <h3>Vendas (Qtd)</h3>
            <h2>{total_qty:,.0f}</h2>
            <div class="delta">Unidades vendidas</div>
        </div>
        """, unsafe_allow_html=True)

    with c3:
        st.markdown(f"""
        <div class="metric-card">
            <h3>Ticket Médio (USD)</h3>
            <h2>$ {avg_ticket:,.2f}</h2>
            <div class="delta">Por transação</div>
        </div>
        """, unsafe_allow_html=True)

    with c4:
        transactions = df.shape[0]
        st.markdown(f"""
        <div class="metric-card">
            <h3>Pedidos</h3>
            <h2>{transactions:,}</h2>
            <div class="delta">Volume total</div>
        </div>
        """, unsafe_allow_html=True)

    return total_faturamento, total_qty, avg_ticket


# --- Bloco 7: Função de Geração de Relatório PDF ---

def dsa_gera_pdf_report(df_dsa_filtrado, total_faturamento, total_quantidade, avg_ticket):
    """
    Gera relatório PDF com KPIs e Top 15 vendas usando FPDF.
    Retorna bytes do PDF para o botão de download.
    """

    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()

    # Título
    pdf.set_font("Helvetica", "B", 16)
    pdf.cell(0, 10, "Analise de Varejo Americano - Superstore", align="C", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.ln(2)

    # Subtítulo com contexto do dataset
    pdf.set_font("Helvetica", "I", 10)
    pdf.cell(0, 6, "Dataset: Superstore Sales (Tableau) | Varejo Americano | 2016-2019", align="C", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.ln(3)

    pdf.set_font("Helvetica", "", 10)
    pdf.cell(0, 8, f"Gerado em: {datetime.now().strftime('%d/%m/%Y %H:%M')}", new_x=XPos.LMARGIN, new_y=YPos.NEXT)

    # Bloco de KPIs
    pdf.set_fill_color(240, 240, 240)
    pdf.rect(10, 42, 190, 25, 'F')
    pdf.set_y(47)

    pdf.set_font("Helvetica", "B", 12)
    pdf.cell(60, 8, "Receita Total (USD)", align="C", new_x=XPos.RIGHT, new_y=YPos.TOP)
    pdf.cell(60, 8, "Quantidade",          align="C", new_x=XPos.RIGHT, new_y=YPos.TOP)
    pdf.cell(60, 8, "Ticket Medio (USD)",  align="C", new_x=XPos.LMARGIN, new_y=YPos.NEXT)

    pdf.set_font("Helvetica", "", 12)
    pdf.cell(60, 8, f"$ {total_faturamento:,.2f}", align="C", new_x=XPos.RIGHT, new_y=YPos.TOP)
    pdf.cell(60, 8, f"{total_quantidade:,}",        align="C", new_x=XPos.RIGHT, new_y=YPos.TOP)
    pdf.cell(60, 8, f"$ {avg_ticket:,.2f}",         align="C", new_x=XPos.LMARGIN, new_y=YPos.NEXT)

    pdf.ln(15)

    # Tabela Top 15
    pdf.set_font("Helvetica", "B", 12)
    pdf.cell(0, 8, "Top 15 Vendas (por receita):", new_x=XPos.LMARGIN, new_y=YPos.NEXT)

    col_widths = [30, 30, 30, 40, 25, 30]
    headers    = ["Data", "Regiao", "Categoria", "Produto", "Qtd", "Receita"]

    pdf.set_font("Helvetica", "B", 9)
    for i, h in enumerate(headers):
        pdf.cell(col_widths[i], 8, h, 1, align='C', new_x=XPos.RIGHT, new_y=YPos.TOP)
    pdf.ln()

    pdf.set_font("Helvetica", "", 9)
    df_top = df_dsa_filtrado.sort_values("faturamento", ascending=False).head(15)

    for _, row in df_top.iterrows():
        data = [
            str(row['date'].date()),
            row['regiao'],
            row['categoria'],
            row['produto'][:20],
            str(row['quantidade']),
            f"$ {row['faturamento']:,.2f}"
        ]
        for i, d in enumerate(data):
            safe_txt = str(d).encode("latin-1", "replace").decode("latin-1")
            pdf.cell(col_widths[i], 7, safe_txt, 1, align=('C' if i == 4 else 'L'), new_x=XPos.RIGHT, new_y=YPos.TOP)
        pdf.ln()

    result = pdf.output()
    return result.encode("latin-1") if isinstance(result, str) else bytes(result)


# --- Bloco 8: Função de Estilização (Tema Customizado) ---

def dsa_set_custom_theme():
    """
    Injeta CSS customizado para estilizar cards de KPI e filtros.
    """

    card_bg_color = "#262730"
    text_color    = "#FAFAFA"
    gold_color    = "#E1C16E"
    dark_text     = "#1E1E1E"

    css = f"""
    <style>
        [data-testid="stMultiSelect"] div[data-baseweb="select"] > div:first-child {{
            min-height: 100px !important;
            overflow-y: auto !important;
        }}

        .metric-card {{
            background-color: {card_bg_color};
            padding: 20px;
            border-radius: 10px;
            border: 1px solid #444;
            box-shadow: 2px 2px 5px rgba(0,0,0,0.3);
            text-align: center;
            margin-bottom: 10px;
        }}
        .metric-card h3 {{
            margin: 0;
            font-size: 1.2rem;
            color: #AAA;
            font-weight: normal;
        }}
        .metric-card h2 {{
            margin: 10px 0 0 0;
            font-size: 2rem;
            color: {text_color};
            font-weight: bold;
        }}
        .metric-card .delta {{
            font-size: 0.9rem;
            color: #4CAF50;
            margin-top: 5px;
        }}

        [data-baseweb="tag"] {{
            background-color: {gold_color} !important;
            color: {dark_text} !important;
            border-radius: 4px !important;
        }}
        [data-baseweb="tag"] svg {{
            color: {dark_text} !important;
        }}
        [data-baseweb="tag"] svg:hover {{
            color: #FF0000 !important;
        }}
    </style>
    """

    st.markdown(css, unsafe_allow_html=True)


# --- Bloco 9: Função Principal ---

def datascienceacademy_mp10():

    dsa_set_custom_theme()

    # CORREÇÃO #2: FileNotFoundError capturado aqui, fora do cache
    try:
        df = dsa_carrega_dados()
    except FileNotFoundError as e:
        st.error(f"❌ {e}")
        st.stop()

    df_dsa_filtrado = dsa_filtros_sidebar(df)

    # --- Cabeçalho ---
    st.title("🛒 Análise de Varejo Americano — Superstore")

    st.markdown("""
    > **Sobre o dataset:** O *Superstore Sales* é um dataset público criado pela **Tableau** que simula
    > as operações de uma rede varejista americana com vendas de **Móveis**, **Material de Escritório** e
    > **Tecnologia** nas regiões dos Estados Unidos entre **2016 e 2019**.
    > É amplamente utilizado para prática de análise de dados e visualização.
    """)

    st.write("Use os filtros na barra lateral para segmentar por período, região, categoria e produto. Os dados podem ser exportados em CSV e PDF.")
    st.markdown("---")
    st.markdown("**Visão Consolidada de Vendas com KPIs**")

    if df_dsa_filtrado.empty:
        st.warning("⚠️ Nenhum dado encontrado com os filtros selecionados.")
        return

    total_faturamento, total_qty, avg_ticket = dsa_renderiza_cards_kpis(df_dsa_filtrado)

    st.markdown("---")

    tab1, tab2 = st.tabs(["📈 Visão Gráfica", "📄 Dados Detalhados & Exportação (CSV e PDF)"])

    # --- Aba 1: Gráficos ---
    with tab1:

        col_left, col_right = st.columns([2, 1])

        with col_left:
            st.subheader("Evolução da Receita Diária (USD)")
            daily_rev = df_dsa_filtrado.groupby("date")[["faturamento"]].sum().reset_index()
            fig_line  = px.line(
                daily_rev, x="date", y="faturamento",
                template="plotly_dark", height=400,
                labels={"date": "Data", "faturamento": "Receita (USD)"}
            )
            fig_line.update_traces(fill='tozeroy', line=dict(color='#0066CC', width=3))
            # CORREÇÃO #4: use_container_width=True substitui width='stretch'
            st.plotly_chart(fig_line, use_container_width=True)

        with col_right:
            st.subheader("Mix de Categorias")
            cat_rev = df_dsa_filtrado.groupby("categoria")[["faturamento"]].sum().reset_index()
            fig_pie = px.pie(
                cat_rev, values="faturamento", names="categoria",
                hole=0.4, template="plotly_dark", height=400
            )
            st.plotly_chart(fig_pie, use_container_width=True)

        c_a, c_b = st.columns(2)

        with c_a:
            st.subheader("Performance por Região (EUA)")
            fig_bar = px.bar(
                df_dsa_filtrado.groupby("regiao")[["faturamento"]].sum().reset_index(),
                x="regiao", y="faturamento", color="regiao",
                template="plotly_dark", text_auto='.2s',
                labels={"regiao": "Região", "faturamento": "Receita (USD)"}
            )
            st.plotly_chart(fig_bar, use_container_width=True)

        with c_b:
            st.subheader("Receita Média por Dia da Semana")

            dias_pt_map = {
                0: "Segunda-feira", 1: "Terça-feira",  2: "Quarta-feira",
                3: "Quinta-feira",  4: "Sexta-feira",  5: "Sábado", 6: "Domingo"
            }
            dias_pt_ordem = [
                "Segunda-feira", "Terça-feira", "Quarta-feira",
                "Quinta-feira",  "Sexta-feira", "Sábado", "Domingo"
            ]

            # CORREÇÃO #3: operações em cópia local para evitar SettingWithCopyWarning
            df_week = df_dsa_filtrado.copy()
            df_week["weekday_num"] = df_week["date"].dt.dayofweek
            df_week["dia_semana"]  = df_week["weekday_num"].map(dias_pt_map)

            wd_rev = (
                df_week.groupby("dia_semana")[["faturamento"]]
                .mean()
                .reindex(dias_pt_ordem)
                .reset_index()
            )

            fig_heat = px.bar(
                wd_rev, x="dia_semana", y="faturamento",
                title="Receita Média (USD) × Dia da Semana",
                template="plotly_dark",
                labels={"dia_semana": "Dia", "faturamento": "Receita Média (USD)"}
            )
            st.plotly_chart(fig_heat, use_container_width=True)

        st.subheader("Dispersão: Quantidade Vendida × Receita por Categoria")
        fig_scat = px.scatter(
            df_dsa_filtrado, x="quantidade", y="faturamento",
            color="categoria", size="faturamento",
            hover_data=["produto"],
            template="plotly_dark", height=500,
            labels={
                "quantidade" : "Quantidade",
                "faturamento": "Receita (USD)",
                "categoria"  : "Categoria"
            }
        )
        st.plotly_chart(fig_scat, use_container_width=True)

    # --- Aba 2: Dados e Exportação ---
    with tab2:

        st.subheader("Visualização Tabular dos Pedidos")
        # CORREÇÃO #4: use_container_width=True
        st.dataframe(df_dsa_filtrado, use_container_width=True, height=400)

        st.markdown("### 📥 Área de Exportação")

        c_exp1, c_exp2 = st.columns(2)

        with c_exp1:
            csv = df_dsa_filtrado.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="💾 Baixar CSV",
                data=csv,
                file_name="superstore_filtrado.csv",
                mime="text/csv",
                use_container_width=True
            )

        with c_exp2:
            if st.button("📄 Gerar Relatório PDF", use_container_width=True):
                with st.spinner("Renderizando PDF..."):
                    pdf_bytes = dsa_gera_pdf_report(
                        df_dsa_filtrado, total_faturamento, total_qty, avg_ticket
                    )
                    st.download_button(
                        label="⬇️ Clique aqui para Salvar PDF",
                        data=pdf_bytes,
                        file_name=f"Relatorio_Superstore_{date.today()}.pdf",
                        mime="application/pdf",
                        key="pdf-download-final"
                    )

    # --- Rodapé ---
    st.markdown("---")

    with st.expander("ℹ️ Sobre Esta Data App", expanded=False):
        st.info("Este dashboard combina as melhores práticas de visualização e manipulação de dados.")
        st.markdown("""
        **Recursos Integrados:**
        - **Dataset:** Superstore Sales — Tableau (varejo americano, 2016–2019).
        - **Engine:** Python + Streamlit + SQLite.
        - **Visualização:** Plotly Express com tema Dark.
        - **Relatórios:** Geração de PDF com FPDF (compatível com Latin-1).
        - **Performance:** Cache de dados (`@st.cache_data`).
        """)


# --- Bloco 10: Ponto de Entrada ---

if __name__ == "__main__":
    datascienceacademy_mp10()


# Obrigado DSA