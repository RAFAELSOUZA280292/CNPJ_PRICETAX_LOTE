import streamlit as st
import requests
import re
import pandas as pd
import time
import datetime
import io
import math
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from zoneinfo import ZoneInfo
from typing import Dict, Tuple, Any, Optional, List

# =========================
# Config da Aplicação
# =========================
st.set_page_config(
    page_title="Consulta CNPJ em Lote - Adapta (Turbo)",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# =========================
# Estilo (tema escuro)
# =========================
st.markdown("""
<style>
    .stApp { background-color: #1A1A1A; color: #EEEEEE; }
    h1, h2, h3, h4, h5, h6 { color: #FFC300; }
    .stTextInput label, .stTextArea label { color: #FFC300; }
    .stTextInput div[data-baseweb="input"] > div, .stTextArea div[data-baseweb="textarea"] > textarea {
        background-color: #333333; color: #EEEEEE; border: 1px solid #FFC300;
    }
    .stTextInput div[data-baseweb="input"] > div:focus-within, .stTextArea div[data-baseweb="textarea"] > textarea:focus-within {
        border-color: #FFD700; box-shadow: 0 0 0 0.1rem rgba(255, 195, 0, 0.25);
    }
    .stButton > button {
        background-color: #FFC300; color: #1A1A1A; border: none; padding: 10px 20px; border-radius: 5px; font-weight: bold;
    }
    .stButton > button:hover { background-color: #FFD700; color: #000000; }
    hr { border-top: 1px solid #444444; }
</style>
""", unsafe_allow_html=True)

# =========================
# Constantes / Globals
# =========================
URL_BRASILAPI_CNPJ = "https://brasilapi.com.br/api/cnpj/v1/"
BRASILIA_TZ = ZoneInfo("America/Sao_Paulo")

# Concorrência inicial e limites
DEFAULT_MAX_WORKERS = 3          # 2-4 é seguro; adaptativo cuida do resto
MIN_INTERVAL_FLOOR = 0.75        # seg: piso de intervalo entre requisições globais
MIN_INTERVAL_CEIL = 5.0          # seg: teto quando API reclama
ADAPT_SUCC_WINDOW = 18           # após este nº de sucessos, tentamos reduzir o intervalo
ADAPT_FAIL_BACKOFF = 2.0         # multiplicador quando erro 429/503/timeout
TOTAL_RETRIES = 3                # tentativas por CNPJ
REQ_TIMEOUT = 20                 # timeout em segundos da requests.get

# Limite de inputs
MAX_INPUTS = 1000

# =========================
# Helpers
# =========================
def limpar_cnpj(cnpj: str) -> str:
    return re.sub(r'[^0-9]', '', cnpj or "")

def calcular_digitos_verificadores_cnpj(cnpj_base_12_digitos: str) -> str:
    pesos_12 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    pesos_13 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]

    def dv(base, pesos):
        s = sum(int(base[i]) * pesos[i] for i in range(len(base)))
        r = s % 11
        return '0' if r < 2 else str(11 - r)

    d13 = dv(cnpj_base_12_digitos[:12], pesos_12)
    d14 = dv(cnpj_base_12_digitos[:12] + d13, pesos_13)
    return d13 + d14

def to_matriz_if_filial(cnpj_clean: str) -> str:
    if len(cnpj_clean) != 14:
        return cnpj_clean
    if cnpj_clean[8:12] != "0001":
        raiz = cnpj_clean[:8]
        base12 = raiz + "0001"
        dvs = calcular_digitos_verificadores_cnpj(base12)
        return base12 + dvs
    return cnpj_clean

def get_regime_tributario(regimes_list: Any) -> Tuple[str, str]:
    if not isinstance(regimes_list, list) or not regimes_list:
        return "N/A", "N/A"
    regimes_por_ano = {r.get('ano'): r.get('forma_de_tributacao') for r in regimes_list if isinstance(r, dict)}
    current_year = datetime.datetime.now().year
    for y in [current_year - i for i in range(6)]:
        if y in regimes_por_ano and regimes_por_ano[y]:
            return regimes_por_ano[y], str(y)
    latest = max((r for r in regimes_list if isinstance(r, dict) and r.get('ano') is not None), key=lambda x: x['ano'], default=None)
    if latest:
        return latest.get('forma_de_tributacao', "N/A"), str(latest.get('ano', "N/A"))
    return "N/A", "N/A"

def extrair_cnaes(api_data: Dict[str, Any]) -> Tuple[str, str]:
    # Principal
    cnae_pri_cod = api_data.get("cnae_fiscal")
    cnae_pri_desc = api_data.get("cnae_fiscal_descricao")
    if cnae_pri_cod and cnae_pri_desc:
        cnae_principal = f"{cnae_pri_cod} - {cnae_pri_desc}"
    elif cnae_pri_cod:
        cnae_principal = str(cnae_pri_cod)
    else:
        cnae_principal = "N/A"
    # Secundário (primeiro)
    sec_list = api_data.get("cnaes_secundarios", []) or []
    cnae_sec = "N/A"
    if isinstance(sec_list, list) and sec_list:
        s0 = sec_list[0] or {}
        c, d = s0.get("codigo"), s0.get("descricao")
        if c and d:
            cnae_sec = f"{c} - {d}"
        elif c:
            cnae_sec = str(c)
    return cnae_principal, cnae_sec

# =========================
# Rate Limiter Adaptativo (global)
# =========================
@dataclass
class AdaptiveLimiter:
    min_interval: float = 1.0  # começa otimista (1 req/seg global, com 3 workers ~ 3 rps)
    last_request_ts: float = 0.0
    successes_since_last_adjust: int = 0

    def __post_init__(self):
        self._lock = st.session_state.get("_limiter_lock")
        if self._lock is None:
            import threading
            self._lock = threading.Lock()
            st.session_state["_limiter_lock"] = self._lock

    def wait_turn(self):
        # Garante espaçamento mínimo global (entre todas as threads)
        with self._lock:
            now = time.time()
            wait_for = (self.last_request_ts + self.min_interval) - now
            if wait_for > 0:
                time.sleep(wait_for)
            self.last_request_ts = time.time()

    def penalize(self):
        # Dobrar intervalo até teto
        with self._lock:
            self.min_interval = min(self.min_interval * ADAPT_FAIL_BACKOFF, MIN_INTERVAL_CEIL)
            self.successes_since_last_adjust = 0

    def reward(self):
        # Contabiliza sucesso; a cada janela, tenta reduzir intervalo até o piso
        with self._lock:
            self.successes_since_last_adjust += 1
            if self.successes_since_last_adjust >= ADAPT_SUCC_WINDOW:
                self.min_interval = max(self.min_interval * 0.85, MIN_INTERVAL_FLOOR)
                self.successes_since_last_adjust = 0

# =========================
# Cache (sessão + @cache_data opcional)
# =========================
if "cache_cnpj" not in st.session_state:
    st.session_state.cache_cnpj: Dict[str, Dict[str, Any]] = {}

@st.cache_data(show_spinner=False, ttl=3600)  # 1h opcional (desative se preferir só cache de sessão)
def cache_disk_get(cnpj: str) -> Optional[Dict[str, Any]]:
    return None  # placeholder para compatibilidade

@st.cache_data(show_spinner=False, ttl=3600)
def cache_disk_put(cnpj: str, data: Dict[str, Any]) -> Dict[str, Any]:
    return data

def cache_get(cnpj: str) -> Optional[Dict[str, Any]]:
    # 1) sessão
    data = st.session_state.cache_cnpj.get(cnpj)
    if data is not None:
        return data
    # 2) disco (comentado por padrão; o wrapper já existe)
    _ = cache_disk_get(cnpj)  # sempre None (mantido para fácil ativação futura)
    return None

def cache_set(cnpj: str, data: Dict[str, Any]):
    st.session_state.cache_cnpj[cnpj] = data
    cache_disk_put(cnpj, data)

# =========================
# Requisição com retry/backoff
# =========================
def request_cnpj_with_retry(cnpj_query: str, limiter: AdaptiveLimiter) -> Tuple[Optional[Dict[str, Any]], Dict[str, int], Optional[str]]:
    """
    Retorna (api_data, metrics_dict, error_msg)
    metrics_dict: {"retries": n, "penalties": m}
    """
    metrics = {"retries": 0, "penalties": 0}
    last_err = None
    for attempt in range(1, TOTAL_RETRIES + 1):
        # Espaçamento global entre requisições
        limiter.wait_turn()
        try:
            resp = requests.get(f"{URL_BRASILAPI_CNPJ}{cnpj_query}", timeout=REQ_TIMEOUT)
            # Respeita Retry-After se presente em 429/503
            if resp.status_code in (429, 503):
                metrics["retries"] += 1
                metrics["penalties"] += 1
                limiter.penalize()
                retry_after = resp.headers.get("Retry-After")
                if retry_after:
                    try:
                        sleep_s = int(retry_after)
                    except Exception:
                        sleep_s = min(max(limiter.min_interval, 2.0), MIN_INTERVAL_CEIL)
                else:
                    # backoff exponencial + jitter
                    base = max(limiter.min_interval, 1.0)
                    sleep_s = min(base * (2 ** (attempt - 1)) + random.uniform(0, 0.5), MIN_INTERVAL_CEIL)
                time.sleep(sleep_s)
                last_err = f"HTTP {resp.status_code}"
                continue

            resp.raise_for_status()
            data = resp.json()
            limiter.reward()
            return data, metrics, None

        except requests.exceptions.Timeout:
            metrics["retries"] += 1
            metrics["penalties"] += 1
            limiter.penalize()
            # backoff com jitter
            base = max(limiter.min_interval, 1.2)
            time.sleep(min(base * (2 ** (attempt - 1)) + random.uniform(0, 0.5), MIN_INTERVAL_CEIL))
            last_err = "Timeout"
        except requests.exceptions.ConnectionError:
            metrics["retries"] += 1
            metrics["penalties"] += 1
            limiter.penalize()
            base = max(limiter.min_interval, 1.2)
            time.sleep(min(base * (2 ** (attempt - 1)) + random.uniform(0, 0.5), MIN_INTERVAL_CEIL))
            last_err = "ConnectionError"
        except requests.exceptions.HTTPError as e:
            # Erros 4xx/5xx (exceto 429/503 que já foram tratados acima)
            last_err = f"HTTP {e.response.status_code if e.response is not None else 'Error'}"
            return None, metrics, last_err
        except Exception as e:
            last_err = f"Erro Inesperado: {e}"
            return None, metrics, last_err

    return None, metrics, last_err

# =========================
# Pipeline de 1 CNPJ
# =========================
def process_one_cnpj(original_cnpj_str: str, limiter: AdaptiveLimiter) -> Dict[str, Any]:
    cleaned = limpar_cnpj(original_cnpj_str)
    if not cleaned or len(cleaned) != 14:
        return {
            "CNPJ": original_cnpj_str, "Razao Social": "CNPJ inválido",
            "Nome Fantasia": 'N/A', "UF": 'N/A',
            "Simples Nacional": 'N/A', "MEI": 'N/A',
            "Regime Tributario": 'N/A', "Ano Regime Tributario": 'N/A',
            "CNAE Principal": 'N/A', "CNAE Secundario (primeiro)": 'N/A',
            "_retries": 0, "_penalties": 0, "_status": "invalid"
        }

    # Cache por sessão
    cached = cache_get(cleaned)
    if cached is not None:
        # Mantém o CNPJ original digitado
        out = dict(cached)
        out["CNPJ"] = original_cnpj_str
        return out

    cnpj_to_query = to_matriz_if_filial(cleaned)
    api_data, metrics, err_msg = request_cnpj_with_retry(cnpj_to_query, limiter)

    if api_data and "cnpj" in api_data:
        forma, ano = get_regime_tributario(api_data.get("regime_tributario", []))
        cnae_pri, cnae_sec = extrair_cnaes(api_data)
        row = {
            "CNPJ": original_cnpj_str,
            "Razao Social": api_data.get('razao_social', 'N/A'),
            "Nome Fantasia": api_data.get('nome_fantasia', 'N/A'),
            "UF": api_data.get('uf', 'N/A'),
            "Simples Nacional": "SIM" if api_data.get('opcao_pelo_simples') else ("NÃO" if api_data.get('opcao_pelo_simples') is False else "N/A"),
            "MEI": "SIM" if api_data.get('opcao_pelo_mei') else ("NÃO" if api_data.get('opcao_pelo_mei') is False else "N/A"),
            "Regime Tributario": forma,
            "Ano Regime Tributario": ano,
            "CNAE Principal": cnae_pri,
            "CNAE Secundario (primeiro)": cnae_sec,
            "_retries": metrics["retries"], "_penalties": metrics["penalties"], "_status": "ok"
        }
        # Cacheia pelo CNPJ LIMPO de entrada (não pela matriz)
        cache_set(cleaned, row)
        return row

    # Caso de erro (HTTP, timeout, etc.)
    msg = err_msg or "Falha desconhecida"
    return {
        "CNPJ": original_cnpj_str,
        "Razao Social": msg,
        "Nome Fantasia": 'N/A', "UF": 'N/A',
        "Simples Nacional": 'N/A', "MEI": 'N/A',
        "Regime Tributario": 'N/A', "Ano Regime Tributario": 'N/A',
        "CNAE Principal": 'N/A', "CNAE Secundario (primeiro)": 'N/A',
        "_retries": metrics["retries"], "_penalties": metrics["penalties"], "_status": "error"
    }

# =========================
# UI
# =========================
st.markdown("<h1 style='text-align: center;'>Consulta de CNPJ em Lote (Turbo)</h1>", unsafe_allow_html=True)
st.markdown("<h3 style='text-align: center;'>Cole até 1.000 CNPJs (um por linha, vírgula, ponto e vírgula, ou espaço)</h3>", unsafe_allow_html=True)

with st.expander("⚙️ Ajustes avançados (opcionais)", expanded=False):
    max_workers = st.slider("Concorrência (threads)", min_value=1, max_value=8, value=DEFAULT_MAX_WORKERS, help="2–4 é geralmente seguro")
    start_interval = st.slider("Intervalo mínimo global inicial (segundos)", min_value=0.5, max_value=3.0, value=1.0, step=0.1,
                               help="O controle é adaptativo; este é só o ponto de partida.")

cnpjs_input = st.text_area(
    "CNPJs (um por linha, ou separados por vírgula, ponto e vírgula ou espaço):",
    height=220,
    placeholder="Ex:\n00.000.000/0001-00\n11.111.111/1111-11\n22.222.222/2222-22"
)

if st.button("🔱 Consultar em Lote"):
    if not cnpjs_input.strip():
        st.warning("Por favor, insira os CNPJs para consultar.")
        st.stop()

    raw = re.split(r'[\n,;\s]+', cnpjs_input.strip())
    cleaned_all = [limpar_cnpj(x) for x in raw if limpar_cnpj(x)]
    if not cleaned_all:
        st.error("Nenhum CNPJ válido encontrado.")
        st.stop()

    # Mantém duplicatas de input? Vamos deduplicar para economizar quota.
    # Continuamos retornando o CNPJ original na linha, então o usuário não perde referência.
    uniq_inputs = list(dict.fromkeys(raw))  # preserva ordem
    # mas só vamos submeter os válidos/limpos (a função lida com inválidos também):
    if len(uniq_inputs) > MAX_INPUTS:
        st.error(f"Você enviou {len(uniq_inputs)} entradas. O limite deste app é {MAX_INPUTS}.")
        st.stop()

    total = len(uniq_inputs)
    st.info(f"Processando **{total}** CNPJs com até **{max_workers}** threads. O controle é **adaptativo**: se a API responder com 429/503/timeouts, "
            f"reduziremos o ritmo automaticamente. 😉")
    progress = st.progress(0)
    eta_box = st.empty()
    stats_box = st.empty()

    limiter = AdaptiveLimiter(min_interval=float(start_interval))

    results: List[Dict[str, Any]] = []
    started_at = time.time()

    # Dispara paralelo
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        fut_map = {ex.submit(process_one_cnpj, cnpj, limiter): cnpj for cnpj in uniq_inputs}
        done_count = 0
        succ = err = 0
        retries_total = penalties_total = 0

        for fut in as_completed(fut_map):
            row = fut.result()
            results.append(row)
            done_count += 1
            succ += 1 if row.get("_status") == "ok" else 0
            err  += 1 if row.get("_status") != "ok" and row.get("_status") != "invalid" else 0
            retries_total += int(row.get("_retries", 0))
            penalties_total += int(row.get("_penalties", 0))

            # Atualiza progresso/ETA
            progress.progress(done_count / total)
            elapsed = time.time() - started_at
            remaining = total - done_count
            eff_rate = done_count / elapsed if elapsed > 0 else 0.0001
            eta_sec = remaining / eff_rate if eff_rate > 0 else 0
            finish_time = datetime.datetime.now(BRASILIA_TZ) + datetime.timedelta(seconds=int(eta_sec))
            eta_box.info(
                f"Progresso: **{done_count}/{total}**  •  Taxa efetiva: **{eff_rate:.2f} req/s**  •  "
                f"ETA: **{str(datetime.timedelta(seconds=int(eta_sec)))}** (≈ {finish_time.strftime('%H:%M:%S')})  •  "
                f"Intervalo atual: **{limiter.min_interval:.2f}s**"
            )
            stats_box.write(
                f"✅ Sucesso: **{succ}**  |  ⚠️ Erros: **{err}**  |  🔁 Retries: **{retries_total}**  |  🧯 Penalidades (backoffs): **{penalties_total}**"
            )

    # Ordena resultados pela ordem de entrada
    order_index = {cnpj: i for i, cnpj in enumerate(uniq_inputs)}
    results.sort(key=lambda r: order_index.get(r.get("CNPJ", ""), 10**9))

    # Monta DataFrame limpo (removendo colunas internas)
    if results:
        for r in results:
            r.pop("_retries", None)
            r.pop("_penalties", None)
            r.pop("_status", None)

    df = pd.DataFrame(results, columns=[
        "CNPJ","Razao Social","Nome Fantasia","UF",
        "Simples Nacional","MEI","Regime Tributario","Ano Regime Tributario",
        "CNAE Principal","CNAE Secundario (primeiro)"
    ])

    st.markdown("---")
    st.subheader("Resultados")
    st.dataframe(df, use_container_width=True)

    if not df.empty:
        timestamp = datetime.datetime.now(BRASILIA_TZ).strftime("%Y%m%d_%H%M%S")
        excel_filename = f"CNPJ_Price_Tax_{timestamp}.xlsx"
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name='Resultados CNPJ')
        st.download_button(
            label="📥 Baixar Excel",
            data=output.getvalue(),
            file_name=excel_filename,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            help="Clique para baixar os resultados em .xlsx"
        )
