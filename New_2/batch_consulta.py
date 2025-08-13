import streamlit as st
import requests
import re
import pandas as pd
import time
import datetime
import io
from pathlib import Path
from zoneinfo import ZoneInfo

# --- Configuração da Aplicação ---
st.set_page_config(
    page_title="Consulta CNPJ em Lote - Adapta",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# --- Estilo CSS Personalizado ---
st.markdown(f"""
<style>
    .stApp {{
        background-color: #1A1A1A;
        color: #EEEEEE;
    }}
    h1, h2, h3, h4, h5, h6 {{
        color: #FFC300;
    }}
    .stTextInput label, .stTextArea label {{
        color: #FFC300;
    }}
    .stTextInput div[data-baseweb="input"] > div, .stTextArea div[data-baseweb="textarea"] > textarea {{
        background-color: #333333;
        color: #EEEEEE;
        border: 1px solid #FFC300;
    }}
    .stTextInput div[data-baseweb="input"] > div:focus-within, .stTextArea div[data-baseweb="textarea"] > textarea:focus-within {{
        border-color: #FFD700;
        box-shadow: 0 0 0 0.1rem rgba(255, 195, 0, 0.25);
    }}
    .stButton > button {{
        background-color: #FFC300;
        color: #1A1A1A;
        border: none;
        padding: 10px 20px;
        border-radius: 5px;
        font-weight: bold;
        transition: background-color 0.3s ease;
    }}
    .stButton > button:hover {{
        background-color: #FFD700;
        color: #000000;
    }}
    .stExpander {{
        background-color: #333333;
        border: 1px solid #FFC300;
        border-radius: 5px;
        padding: 10px;
        margin-bottom: 10px;
    }}
    .stExpander > div > div > div > p {{
        color: #EEEEEE;
    }}
    .stAlert {{
        background-color: #333333;
        color: #EEEEEE;
        border-left: 5px solid #FFC300;
        border-radius: 5px;
    }}
    .stAlert > div > div > div > div > span {{
        color: #EEEEEE !important;
    }}
    .stAlert > div > div > div > div > svg {{
        color: #FFC300 !important;
    }}
    hr {{
        border-top: 1px solid #444444;
    }}
</style>
""", unsafe_allow_html=True)

# --- Constantes da API ---
URL_BRASILAPI_CNPJ = "https://brasilapi.com.br/api/cnpj/v1/"
RATE_LIMIT_SECONDS = 60 / 15  # 15 requisições/min -> 1 a cada 4s aprox.

# --- Define o fuso horário de Brasília ---
BRASILIA_TZ = ZoneInfo("America/Sao_Paulo")

# --- Funções Auxiliares ---
def limpar_cnpj(cnpj: str) -> str:
    """Remove caracteres não numéricos do CNPJ."""
    return re.sub(r'[^0-9]', '', cnpj)

def calcular_digitos_verificadores_cnpj(cnpj_base_12_digitos: str) -> str:
    """
    Calcula os dois dígitos verificadores de um CNPJ a partir dos primeiros 12 dígitos (Módulo 11).
    """
    pesos_12_digitos = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    pesos_13_digitos = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]

    def calcula_dv_single(base_str, pesos):
        soma = sum(int(base_str[i]) * pesos[i] for i in range(len(base_str)))
        resto = soma % 11
        return 0 if resto < 2 else 11 - resto

    d13 = calcula_dv_single(cnpj_base_12_digitos[:12], pesos_12_digitos)
    d14 = calcula_dv_single(cnpj_base_12_digitos[:12] + str(d13), pesos_13_digitos)
    return f"{d13}{d14}"

def get_regime_tributario(regimes_list):
    """
    Busca o regime tributário mais próximo ao ano atual.
    Retorna (forma_de_tributacao, ano).
    """
    if not regimes_list:
        return "N/A", "N/A"

    regimes_por_ano = {regime.get('ano'): regime.get('forma_de_tributacao') for regime in regimes_list if isinstance(regime, dict)}
    current_year = datetime.datetime.now().year
    target_years = [current_year - i for i in range(6)]  # ano atual até -5

    for year in target_years:
        if year in regimes_por_ano and regimes_por_ano[year]:
            return regimes_por_ano[year], str(year)

    # Fallback: pega o mais recente disponível
    latest = max((r for r in regimes_list if isinstance(r, dict) and r.get('ano') is not None), key=lambda x: x['ano'], default=None)
    if latest:
        return latest.get('forma_de_tributacao', "N/A"), str(latest.get('ano', "N/A"))
    return "N/A", "N/A"

def extrair_cnaes(api_data: dict):
    """
    Retorna (cnae_principal_str, cnae_secundario_primeiro_str).
    Monta 'codigo - descricao' quando houver ambos.
    """
    # CNAE Principal
    cnae_principal_codigo = api_data.get("cnae_fiscal")
    cnae_principal_desc = api_data.get("cnae_fiscal_descricao")
    if cnae_principal_codigo and cnae_principal_desc:
        cnae_principal = f"{cnae_principal_codigo} - {cnae_principal_desc}"
    elif cnae_principal_codigo:
        cnae_principal = str(cnae_principal_codigo)
    else:
        cnae_principal = "N/A"

    # Primeiro CNAE Secundário
    sec_list = api_data.get("cnaes_secundarios", []) or []
    cnae_secundario_primeiro = "N/A"
    if isinstance(sec_list, list) and sec_list:
        s0 = sec_list[0] or {}
        s0_codigo = s0.get("codigo")
        s0_desc = s0.get("descricao")
        if s0_codigo and s0_desc:
            cnae_secundario_primeiro = f"{s0_codigo} - {s0_desc}"
        elif s0_codigo:
            cnae_secundario_primeiro = str(s0_codigo)

    return cnae_principal, cnae_secundario_primeiro

def consultar_cnpj_lote(cnpjs_list):
    """
    Consulta CNPJs em lote respeitando rate limit.
    Para CNPJ filial, consulta a matriz (raiz + 0001 + DVs).
    """
    resultados = []
    total_cnpjs = len(cnpjs_list)
    start_time = time.time()

    progress_bar = st.progress(0)
    status_text = st.empty()
    time_estimate_text = st.empty()
    current_request_text = st.empty()

    st.info(f"""
**Atenção:** Será feita 1 requisição a cada **{RATE_LIMIT_SECONDS:.0f} segundos** (15 requisições por minuto).
Para **{total_cnpjs} CNPJs**, tempo estimado: **{str(datetime.timedelta(seconds=int(total_cnpjs * RATE_LIMIT_SECONDS)))}**.
Durante o processamento, mantenha a página ativa no navegador.
""", icon="ℹ️")

    for i, original_cnpj_str in enumerate(cnpjs_list):
        progress = (i + 1) / total_cnpjs
        progress_bar.progress(progress)

        elapsed_time = time.time() - start_time
        remaining_cnpjs = total_cnpjs - (i + 1)
        time_per_future_request = max(RATE_LIMIT_SECONDS, elapsed_time / (i + 1) if (i + 1) > 0 else RATE_LIMIT_SECONDS)
        estimated_remaining_seconds = remaining_cnpjs * time_per_future_request
        current_brasilia_time = datetime.datetime.now(BRASILIA_TZ)
        estimated_finish_time = current_brasilia_time + datetime.timedelta(seconds=estimated_remaining_seconds)

        time_estimate_text.info(
            f"Progresso: **{int(progress*100)}%**  \n"
            f"CNPJs restantes: **{remaining_cnpjs}**  \n"
            f"Tempo restante: **{str(datetime.timedelta(seconds=int(estimated_remaining_seconds)))}**  \n"
            f"Conclusão prevista: **{estimated_finish_time.strftime('%H:%M:%S de %d/%m/%Y')}**"
        )
        current_request_text.text(f"Consultando CNPJ: {original_cnpj_str} ({i + 1}/{total_cnpjs})")

        cleaned_original_cnpj = limpar_cnpj(original_cnpj_str)
        cnpj_to_query = cleaned_original_cnpj

        # Se for CNPJ de 14 dígitos e filial, converte para matriz (....0001 + DVs)
        if len(cleaned_original_cnpj) == 14:
            identificador_filial = cleaned_original_cnpj[8:12]
            if identificador_filial != "0001":
                raiz = cleaned_original_cnpj[:8]
                matriz_12 = raiz + "0001"
                dvs = calcular_digitos_verificadores_cnpj(matriz_12)
                cnpj_to_query = matriz_12 + dvs

        dados_cnpj_linha = {}
        try:
            response = requests.get(f"{URL_BRASILAPI_CNPJ}{cnpj_to_query}", timeout=15)
            response.raise_for_status()
            api_data = response.json()

            if "cnpj" in api_data:
                forma_tributacao, ano_tributacao = get_regime_tributario(api_data.get('regime_tributario', []))
                cnae_principal, cnae_secundario_primeiro = extrair_cnaes(api_data)

                dados_cnpj_linha = {
                    "CNPJ": original_cnpj_str,
                    "Razao Social": api_data.get('razao_social', 'N/A'),
                    "Nome Fantasia": api_data.get('nome_fantasia', 'N/A'),
                    "UF": api_data.get('uf', 'N/A'),
                    "Simples Nacional": "SIM" if api_data.get('opcao_pelo_simples') else ("NÃO" if api_data.get('opcao_pelo_simples') is False else "N/A"),
                    "MEI": "SIM" if api_data.get('opcao_pelo_mei') else ("NÃO" if api_data.get('opcao_pelo_mei') is False else "N/A"),
                    "Regime Tributario": forma_tributacao,
                    "Ano Regime Tributario": ano_tributacao,
                    "CNAE Principal": cnae_principal,
                    "CNAE Secundario (primeiro)": cnae_secundario_primeiro,
                }
            else:
                # Retorno com mensagem de erro específica
                dados_cnpj_linha = {
                    "CNPJ": original_cnpj_str,
                    "Razao Social": api_data.get('message', f'CNPJ não encontrado: {original_cnpj_str}'),
                    "Nome Fantasia": 'N/A',
                    "UF": 'N/A',
                    "Simples Nacional": 'N/A',
                    "MEI": 'N/A',
                    "Regime Tributario": 'N/A',
                    "Ano Regime Tributario": 'N/A',
                    "CNAE Principal": 'N/A',
                    "CNAE Secundario (primeiro)": 'N/A',
                }

        except requests.exceptions.Timeout:
            dados_cnpj_linha = {
                "CNPJ": original_cnpj_str,
                "Razao Social": "Timeout da Requisição",
                "Nome Fantasia": 'N/A',
                "UF": 'N/A',
                "Simples Nacional": 'N/A',
                "MEI": 'N/A',
                "Regime Tributario": 'N/A',
                "Ano Regime Tributario": 'N/A',
                "CNAE Principal": 'N/A',
                "CNAE Secundario (primeiro)": 'N/A',
            }
        except requests.exceptions.ConnectionError:
            dados_cnpj_linha = {
                "CNPJ": original_cnpj_str,
                "Razao Social": "Erro de Conexão",
                "Nome Fantasia": 'N/A',
                "UF": 'N/A',
                "Simples Nacional": 'N/A',
                "MEI": 'N/A',
                "Regime Tributario": 'N/A',
                "Ano Regime Tributario": 'N/A',
                "CNAE Principal": 'N/A',
                "CNAE Secundario (primeiro)": 'N/A',
            }
        except requests.exceptions.HTTPError as e:
            dados_cnpj_linha = {
                "CNPJ": original_cnpj_str,
                "Razao Social": f"Erro HTTP {e.response.status_code}",
                "Nome Fantasia": 'N/A',
                "UF": 'N/A',
                "Simples Nacional": 'N/A',
                "MEI": 'N/A',
                "Regime Tributario": 'N/A',
                "Ano Regime Tributario": 'N/A',
                "CNAE Principal": 'N/A',
                "CNAE Secundario (primeiro)": 'N/A',
            }
        except Exception as e:
            dados_cnpj_linha = {
                "CNPJ": original_cnpj_str,
                "Razao Social": f"Erro Inesperado: {e}",
                "Nome Fantasia": 'N/A',
                "UF": 'N/A',
                "Simples Nacional": 'N/A',
                "MEI": 'N/A',
                "Regime Tributario": 'N/A',
                "Ano Regime Tributario": 'N/A',
                "CNAE Principal": 'N/A',
                "CNAE Secundario (primeiro)": 'N/A',
            }

        resultados.append(dados_cnpj_linha)

        # Respeita rate limit entre chamadas
        if i < total_cnpjs - 1:
            time_to_wait = RATE_LIMIT_SECONDS - (time.time() - start_time) % RATE_LIMIT_SECONDS
            if 0 < time_to_wait < RATE_LIMIT_SECONDS:
                time.sleep(time_to_wait)

    progress_bar.empty()
    status_text.empty()
    time_estimate_text.empty()
    current_request_text.empty()
    return pd.DataFrame(resultados)

# --- UI Principal ---
st.markdown("<h1 style='text-align: center;'>Consulta de CNPJ em Lote</h1>", unsafe_allow_html=True)
st.markdown("<h3 style='text-align: center;'>Colar até 500 CNPJs (um por linha, ou separados por vírgula, ponto e vírgula, ou espaço)</h3>", unsafe_allow_html=True)

cnpjs_input = st.text_area(
    "Insira os CNPJs (um por linha, ou separados por vírgula, ponto e vírgula ou espaço):",
    height=200,
    placeholder="Ex:\n00.000.000/0000-00\n11.111.111/1111-11\n22.222.222/2222-22"
)

if st.button("🔱 Consultar em Lote"):
    if not cnpjs_input:
        st.warning("Por favor, insira os CNPJs para consultar.")
    else:
        cnpjs_raw = re.split(r'[\n,;\s]+', cnpjs_input)
        cnpjs_limpos = [limpar_cnpj(cnpj) for cnpj in cnpjs_raw if limpar_cnpj(cnpj)]
        cnpjs_unicos = list(set(cnpjs_limpos))

        if not cnpjs_unicos:
            st.error("Nenhum CNPJ válido foi encontrado na entrada. Verifique o formato.")
        elif len(cnpjs_unicos) > 500:
            st.error(f"Você inseriu {len(cnpjs_unicos)} CNPJs. O limite é de 500 CNPJs por lote.")
        else:
            st.info(f"Processando {len(cnpjs_unicos)} CNPJs. A barra de progresso e a estimativa serão exibidas.")
            df_resultados = consultar_cnpj_lote(cnpjs_unicos)

            st.markdown("---")
            st.markdown("## Resultados da Consulta em Lote")
            st.dataframe(df_resultados, use_container_width=True)

            if not df_resultados.empty:
                timestamp = datetime.datetime.now(BRASILIA_TZ).strftime("%Y%m%d_%H%M%S")
                excel_filename = f"CNPJ_Price_Tax_{timestamp}.xlsx"

                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    df_resultados.to_excel(writer, index=False, sheet_name='Resultados CNPJ')
                processed_data = output.getvalue()

                st.download_button(
                    label="📥 Baixar Resultados como Excel",
                    data=processed_data,
                    file_name=excel_filename,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    help="Clique para baixar os resultados em formato .xlsx"
                )
