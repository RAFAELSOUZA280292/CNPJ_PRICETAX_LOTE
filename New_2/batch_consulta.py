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
    /* Cor de Fundo Principal da Aplicação - Muito Escuro / Quase Preto */
    .stApp {{
        background-color: #1A1A1A; /* Quase preto */
        color: #EEEEEE; /* Cinza claro para o texto principal */
    }}

    /* Títulos (h1 a h6) */
    h1, h2, h3, h4, h5, h6 {{
        color: #FFC300; /* Amarelo Riqueza */
    }}

    /* Estilo dos Labels e Inputs de Texto */
    .stTextInput label, .stTextArea label {{
        color: #FFC300; /* Amarelo Riqueza para os labels */
    }}
    .stTextInput div[data-baseweb="input"] > div, .stTextArea div[data-baseweb="textarea"] > textarea {{
        background-color: #333333; /* Cinza escuro para o fundo do input */
        color: #EEEEEE; /* Cinza claro para o texto digitado */
        border: 1px solid #FFC300; /* Borda Amarelo Riqueza */
    }}
    /* Estilo do input/textarea quando focado */
    .stTextInput div[data-baseweb="input"] > div:focus-within, .stTextArea div[data-baseweb="textarea"] > textarea:focus-within {{
        border-color: #FFD700; /* Amarelo ligeiramente mais claro no foco */
        box-shadow: 0 0 0 0.1rem rgba(255, 195, 0, 0.25); /* Sombra sutil */
    }}

    /* Estilo dos Botões */
    .stButton > button {{
        background-color: #FFC300; /* Amarelo Riqueza */
        color: #1A1A1A; /* Texto escuro no botão amarelo */
        border: none;
        padding: 10px 20px;
        border-radius: 5px;
        font-weight: bold;
        transition: background-color 0.3s ease; /* Transição suave no hover */
    }}
    /* Estilo do botão ao passar o mouse */
    .stButton > button:hover {{
        background-color: #FFD700; /* Amarelo ligeiramente mais claro no hover */
        color: #000000; /* Preto total no texto para contraste */
    }}

    /* Estilo dos Expanders (usado para QSA, por exemplo) */
    .stExpander {{
        background-color: #333333; /* Cinza escuro para o fundo do expander */
        border: 1px solid #FFC300; /* Borda Amarelo Riqueza */
        border-radius: 5px;
        padding: 10px;
        margin-bottom: 10px;
    }}
    .stExpander > div > div > div > p {{
        color: #EEEEEE; /* Cinza claro para o título do expander */
    }}

    /* Estilo para st.info, st.warning, st.error */
    .stAlert {{
        background-color: #333333; /* Cinza escuro para o fundo dos alertas */
        color: #EEEEEE; /* Cinza claro para o texto */
        border-left: 5px solid #FFC300; /* Borda esquerda Amarelo Riqueza */
        border-radius: 5px;
    }}
    .stAlert > div > div > div > div > span {{
        color: #EEEEEE !important; /* Garante que o texto dentro do alerta seja claro */
    }}
    .stAlert > div > div > div > div > svg {{
        color: #FFC300 !important; /* Garante que o ícone do alerta seja amarelo */
    }}

    /* Linhas divisórias */
    hr {{
        border-top: 1px solid #444444; /* Cinza para divisórias */
    }}
</style>
""", unsafe_allow_html=True)

# --- Constantes da API ---
URL_BRASILAPI_CNPJ = "https://brasilapi.com.br/api/cnpj/v1/"
RATE_LIMIT_SECONDS = 60 / 15 # 4 segundos por requisição

# --- Define o fuso horário de Brasília ---
BRASILIA_TZ = ZoneInfo("America/Sao_Paulo")

# --- Funções Auxiliares ---
def limpar_cnpj(cnpj):
    """Remove caracteres não numéricos do CNPJ."""
    return re.sub(r'[^0-9]', '', cnpj)

def calcular_digitos_verificadores_cnpj(cnpj_base_12_digitos):
    """
    Calcula os dois dígitos verificadores de um CNPJ a partir dos primeiros 12 dígitos,
    usando a lógica de Módulo 11 conforme as regras do CNPJ.
    """
    
    # Pesos conforme a lógica do CNPJ, aplicados da esquerda para a direita.
    # Para o primeiro DV (d13), 12 dígitos.
    pesos_12_digitos = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2] 
    # Para o segundo DV (d14), 13 dígitos (incluindo o d13 recém-calculado).
    pesos_13_digitos = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]

    def calcula_dv_single(base_str, pesos):
        soma = 0
        for i in range(len(base_str)):
            soma += int(base_str[i]) * pesos[i]
        
        resto = soma % 11
        if resto < 2:
            return 0
        else:
            return 11 - resto

    # Calcula o primeiro dígito verificador (d13)
    d13 = calcula_dv_single(cnpj_base_12_digitos[:12], pesos_12_digitos)

    # Calcula o segundo dígito verificador (d14)
    cnpj_parte_com_d13 = cnpj_base_12_digitos[:12] + str(d13)
    d14 = calcula_dv_single(cnpj_parte_com_d13, pesos_13_digitos)

    return str(d13) + str(d14)


def get_regime_tributario(regimes_list):
    """
    Busca o regime tributário mais próximo ao ano atual.
    Retorna o regime e o ano.
    """
    if not regimes_list:
        return "N/A", "N/A"

    regimes_por_ano = {regime['ano']: regime['forma_de_tributacao'] for regime in regimes_list}
    current_year = datetime.datetime.now().year
    target_years = [current_year, current_year - 1, current_year - 2, current_year - 3, current_year - 4, current_year - 5]

    for year in target_years:
        if year in regimes_por_ano:
            return regimes_por_ano[year], str(year)
    
    latest_regime = None
    latest_year = None
    for regime in regimes_list:
        if latest_year is None or regime['ano'] > latest_year:
            latest_year = regime['ano']
            latest_regime = regime['forma_de_tributacao']
    
    if latest_regime and latest_year:
        return latest_regime, str(latest_year)

    return "N/A", "N/A"

def consultar_cnpj_lote(cnpjs_list):
    """
    Realiza a consulta de CNPJs em lote com controle de taxa,
    ajustando para consulta de matriz se o CNPJ for filial.
    """
    resultados = []
    total_cnpjs = len(cnpjs_list)
    start_time = time.time()

    progress_bar = st.progress(0)
    status_text = st.empty()
    time_estimate_text = st.empty()
    current_request_text = st.empty()

    st.info(f"""
        **Atenção:** Será feita 1 requisição a cada **{RATE_LIMIT_SECONDS:.0f} segundos** (15 requisições por minuto)
        para respeitar os limites de requisição típicos de APIs públicas.
        Para a sua consulta de lote de **{total_cnpjs} CNPJs**, o tempo estimado de processamento será de
        **{str(datetime.timedelta(seconds=total_cnpjs * RATE_LIMIT_SECONDS))}**.
        Durante o processamento, é importante manter a página ativa no navegador para evitar interrupções.
        """, icon="ℹ️")


    for i, original_cnpj_str in enumerate(cnpjs_list): # original_cnpj_str é o CNPJ como o usuário digitou
        progress = (i + 1) / total_cnpjs
        progress_bar.progress(progress)

        elapsed_time = time.time() - start_time
        remaining_cnpjs = total_cnpjs - (i + 1)
        
        time_per_future_request = max(RATE_LIMIT_SECONDS, elapsed_time / (i + 1) if (i + 1) > 0 else RATE_LIMIT_SECONDS)
        
        estimated_remaining_seconds = remaining_cnpjs * time_per_future_request
        
        current_brasilia_time = datetime.datetime.now(BRASILIA_TZ)
        estimated_finish_time = current_brasilia_time + datetime.timedelta(seconds=estimated_remaining_seconds)
        
        time_estimate_text.info(
            f"Progresso: **{int(progress*100)}%**\n\n"
            f"CNPJs restantes: **{remaining_cnpjs}**\n\n"
            f"Tempo estimado para conclusão: **{str(datetime.timedelta(seconds=estimated_remaining_seconds)).split('.')[0]}**\n\n"
            f"Conclusão esperada por volta de: **{estimated_finish_time.strftime('%H:%M:%S de %d/%m/%Y')}**"
        )
        current_request_text.text(f"Consultando CNPJ: {original_cnpj_str} ({i + 1}/{total_cnpjs})")

        cleaned_original_cnpj = limpar_cnpj(original_cnpj_str)
        cnpj_to_query = cleaned_original_cnpj # Por padrão, consulta o próprio CNPJ

        # Verifica se o CNPJ é válido (14 dígitos) para aplicar a lógica de filial/matriz
        if len(cleaned_original_cnpj) == 14:
            identificador_filial = cleaned_original_cnpj[8:12] # Parte FFFF
            
            # Se não for '0001', é uma filial
            if identificador_filial != "0001":
                cnpj_raiz_8_digitos = cleaned_original_cnpj[:8] # Parte BB.BBB.BBB
                
                # Constrói os primeiros 12 dígitos do CNPJ da matriz (raiz + 0001)
                matriz_12_digitos = cnpj_raiz_8_digitos + "0001"
                
                # Calcula os dígitos verificadores para o CNPJ da matriz
                matriz_dvs = calcular_digitos_verificadores_cnpj(matriz_12_digitos)
                
                # Monta o CNPJ completo da matriz para a consulta
                cnpj_to_query = matriz_12_digitos + matriz_dvs
                # O CNPJ na mensagem de status já é o original.
                # Não exibe mensagem extra para não poluir a tela em lote.
        else:
            # Se o CNPJ original não tiver 14 dígitos, é considerado inválido para busca de matriz
            # e será tratado no bloco de exceção como erro.
            cnpj_to_query = cleaned_original_cnpj # Permanece como o original (inválido)


        dados_cnpj_linha = {}
        try:
            response = requests.get(f"{URL_BRASILAPI_CNPJ}{cnpj_to_query}", timeout=15)
            response.raise_for_status()
            api_data = response.json()

            if "cnpj" in api_data:
                # Extrai o regime tributário
                regime_tributario_info = api_data.get('regime_tributario', [])
                forma_tributacao, ano_tributacao = get_regime_tributario(regime_tributario_info)

                dados_cnpj_linha = {
                    "CNPJ": original_cnpj_str, # AQUI: Garante que o CNPJ original do usuário é mantido
                    "Razao Social": api_data.get('razao_social', 'N/A'),
                    "Nome Fantasia": api_data.get('nome_fantasia', 'N/A'),
                    "UF": api_data.get('uf', 'N/A'),
                    "Simples Nacional": "SIM" if api_data.get('opcao_pelo_simples') else ("NÃO" if api_data.get('opcao_pelo_simples') is False else "N/A"),
                    "MEI": "SIM" if api_data.get('opcao_pelo_mei') else ("NÃO" if api_data.get('opcao_pelo_mei') is False else "N/A"),
                    "Regime Tributario": forma_tributacao,
                    "Ano Regime Tributario": ano_tributacao
                }
            else:
                # Caso a API retorne mensagem de erro específica para o CNPJ consultado (mesmo sendo matriz)
                dados_cnpj_linha = {
                    "CNPJ": original_cnpj_str, # AQUI: Garante que o CNPJ original do usuário é mantido
                    "Razao Social": api_data.get('message', f'CNPJ não encontrado: {original_cnpj_str}'),
                    "Nome Fantasia": 'N/A',
                    "UF": 'N/A',
                    "Simples Nacional": 'N/A',
                    "MEI": 'N/A',
                    "Regime Tributario": 'N/A',
                    "Ano Regime Tributario": 'N/A'
                }

        except requests.exceptions.Timeout:
            dados_cnpj_linha = {
                "CNPJ": original_cnpj_str, "Razao Social": "Timeout da Requisição", "Nome Fantasia": 'N/A', "UF": 'N/A',
                "Simples Nacional": 'N/A', "MEI": 'N/A', "Regime Tributario": 'N/A', "Ano Regime Tributario": 'N/A'
            }
        except requests.exceptions.ConnectionError:
            dados_cnpj_linha = {
                "CNPJ": original_cnpj_str, "Razao Social": "Erro de Conexão", "Nome Fantasia": 'N/A', "UF": 'N/A',
                "Simples Nacional": 'N/A', "MEI": 'N/A', "Regime Tributario": 'N/A', "Ano Regime Tributario": 'N/A'
            }
        except requests.exceptions.HTTPError as e:
            dados_cnpj_linha = {
                "CNPJ": original_cnpj_str, "Razao Social": f"Erro HTTP {e.response.status_code}", "Nome Fantasia": 'N/A', "UF": 'N/A',
                "Simples Nacional": 'N/A', "MEI": 'N/A', "Regime Tributario": 'N/A', "Ano Regime Tributario": 'N/A'
            }
        except Exception as e:
            dados_cnpj_linha = {
                "CNPJ": original_cnpj_str, "Razao Social": f"Erro Inesperado: {e}", "Nome Fantasia": 'N/A', "UF": 'N/A',
                "Simples Nacional": 'N/A', "MEI": 'N/A', "Regime Tributario": 'N/A', "Ano Regime Tributario": 'N/A'
            }
        
        resultados.append(dados_cnpj_linha)

        if i < total_cnpjs - 1:
            time_to_wait = RATE_LIMIT_SECONDS - (time.time() - start_time) % RATE_LIMIT_SECONDS
            if time_to_wait > 0 and time_to_wait < RATE_LIMIT_SECONDS:
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
            st.error(f"Você inseriu {len(cnpjs_unicos)} CNPJs. O limite é de 500 CNPJs por lote para garantir a performance e respeitar limites da API.")
        else:
            st.info(f"Processando {len(cnpjs_unicos)} CNPJs. Isso pode levar algum tempo, mas a barra de progresso e estimativa serão exibidas.")
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
