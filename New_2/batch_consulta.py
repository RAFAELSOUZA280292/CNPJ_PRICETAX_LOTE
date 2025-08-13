import streamlit as st
import requests
import re
import pandas as pd
import time
import datetime
import io
import random
import threading
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
# Constantes / Globais
# =========================
URL_BRASILAPI_CNPJ = "https://brasilapi.com.br/api/cnpj/v1/"
BRASILIA_TZ = ZoneInfo("America/Sao_Paulo")

# Parâmetros FIXOS (rápido e seguro)
MAX_WORKERS = 3           # concorrência fixa
START_INTERVAL = 1.0      # seg — ponto inicial do limitador adaptativo

# Limitador adaptativo – limites
MIN_INTERVAL_FLOOR = 0.75
MIN_INTERVAL_CEIL  = 5.0
ADAPT_SUCC_WINDOW  = 18
ADAPT_FAIL_BACKOFF = 2.0

# Robustez
TOTAL_RETRIES = 3
REQ_TIMEOUT   = 20

# Limite de inputs
MAX_INPUTS = 1000

# ---------- Cache global thread-safe (não usa session_state nas threads) ----------
_CACHE: Dict[str, Dict[str, Any]] = {}
_CACHE_LOCK = threading.Lock()

def cache_get(cnpj: str) -> Optional[Dict[str, Any]]:
    with _CACHE_LOCK:
        return _CACHE.get(cnpj)

def cache_set(cnpj: str, data: Dict[str, Any]) -> None:
    with _CACHE_LOCK:
        _CACHE[cnpj] = data

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
    latest = max((r for r in regimes_list if isinstance(r, dict) and r.get('ano') is not None),
                 key=lambda x: x['ano'], default=None)
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
# Rate Limiter Adaptativo (lock próprio)
# =========================
@dataclass
class AdaptiveLimiter:
    min_interval: float = START_INTERVAL
    last_request_ts: float = 0.0
    successes_since_last_adjust: int = 0

    def __post_init__(self):
        self._lock = threading.Lock()

    def wait_turn(self):
        # espaçamento mínimo global (entre todas as threads que compartilham este objeto)
        with self._lock:
            now = time.time()
            wait_for = (self.last_request_ts + self.min_interval) - now
            if wait_for > 0:
                time.sleep(wait_for)
            self.last_request_ts = time.time()

    def penalize(self):
        with self._lock:
            self.min_interval = min(self.min_interval * ADAPT_FAIL_BACKOFF, MIN_INTERVAL_CEIL)
            self.successes_since_last_adjust = 0

    def reward(self):
        with self._lock:
            self.successes_since_last_adjust += 1
            if self.successes_since_last_adjust >= ADAPT_SUCC_WINDOW:
                self.min_interval = max(self.min_interval * 0.85, MIN_INTERVAL_FLOOR)
                self.successes_since_last_adjust = 0

# =========================
# Requisição com retry/backoff
# =========================
def request_cnpj_with_retry(cnpj_query: str, limiter: AdaptiveLimiter) -> Tuple[Optional[Dict[str, Any]], str]:
    """
    Retorna (api_data, error_msg)
    """
    last_err = None
    for attempt in range(1, TOTAL_RETRIES + 1):
        limiter.wait_turn()
        try:
            resp = requests.get(f"{URL_BRASILAPI_CNPJ}{cnpj_query}", timeout=REQ_TIMEOUT)
            if resp.status_code in (429, 503):
                limiter.penalize()
                retry_after = resp.headers.get("Retry-After")
                if retry_after:
                    try:
                        sleep_s = int(retry_after)
                    except Exception:
                        sleep_s = min(max(limiter.min_interval, 2.0), MIN_INTERVAL_CEIL)
                else:
                    base = max(limiter.min_interval, 1.0)
                    sleep_s = min(base * (2 ** (attempt - 1)) + random.uniform(0, 0.5), MIN_INTERVAL_CEIL)
                time.sleep(sleep_s)
                last_err = f"HTTP {resp.status_code}"
                continue

            resp.raise_for_status()
            data = resp.json()
            limiter.reward()
            return data, None

        except requests.exceptions.Timeout:
            limiter.penalize()
            base = max(limiter.min_interval, 1.2)
            time.sleep(min(base * (2 ** (attempt - 1)) + random.uniform(0, 0.5), MIN_INTERVAL_CEIL))
            last_err = "Timeout"
        except requests.exceptions.ConnectionError:
            limiter.penalize()
            base = max(limiter.min_interval, 1.2)
            time.sleep(min(base * (2 ** (attempt - 1)) + random.uniform(0, 0.5), MIN_INTERVAL_CEIL))
            last_err = "ConnectionError"
        except requests.exceptions.HTTPError as e:
            last_err = f"HTTP {e.response.status_code if e.response is not None else 'Error'}"
            return None, last_err
        except Exception as e:
            last_err = f"Erro Inesperado: {e}"
            return None, last_err

    return None, last_err or "Falha desconhecida"

# =========================
# Pipeline de 1 CNPJ (executa nas threads)
# =========================
def process_one_cnpj(original_cnpj_str: str, limiter: AdaptiveLimiter) -> Dict[str, Any]:
    cleaned = limpar_cnpj(original_cnpj_str)
    if not cleaned or len(cleaned) != 14:
        return {
            "CNPJ": original_cnpj_str, "Razao Social": "CNPJ inválido",
            "Nome Fantasia": 'N/A', "UF": 'N/A',
            "Simples Nacional": 'N/A', "MEI": 'N/A',
            "Regime Tributario": 'N/A', "Ano Regime Tributario": 'N/A',
            "CNAE Principal": 'N/A', "CNAE Secundario (primeiro)": 'N/A'
        }

    # cache por CNPJ limpo (da entrada)
    cached = cache_get(cleaned)
    if cached is not None:
        out = dict(cached)
        out["CNPJ"] = original_cnpj_str
        return out

    cnpj_to_query = to_matriz_if_filial(cleaned)
    api_data, err_msg = request_cnpj_with_retry(cnpj_to_query, limiter)

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
        }
        cache_set(cleaned, row)
        return row

    msg = err_msg or "Falha desconhecida"
    return {
        "CNPJ": original_cnpj_str,
        "Razao Social": msg,
        "Nome Fantasia": 'N/A', "UF": 'N/A',
        "Simples Nacional": 'N/A', "MEI": 'N/A',
        "Regime Tributario": 'N/A', "Ano Regime Tributario": 'N/A',
        "CNAE Principal": 'N/A', "CNAE Secundario (primeiro)": 'N/A'
    }

# =========================
# UI
# =========================
st.markdown("<h1 style='text-align: center;'>Consulta de CNPJ em Lote (Turbo)</h1>", unsafe_allow_html=True)
st.markdown("<h3 style='text-align: center;'>Cole até 1.000 CNPJs (um por linha, vírgula, ponto e vírgula, ou espaço)</h3>", unsafe_allow_html=True)

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
    uniq_inputs = list(dict.fromkeys(raw))  # preserva ordem e remove duplicados exatos
    if len(uniq_inputs) > MAX_INPUTS:
        st.error(f"Você enviou {len(uniq_inputs)} entradas. O limite deste app é {MAX_INPUTS}.")
        st.stop()

    total = len(uniq_inputs)
    st.info(f"Processando **{total}** CNPJs com **{MAX_WORKERS}** threads. O ritmo é controlado automaticamente para evitar bloqueios pela API.")
    progress = st.progress(0)
    eta_box = st.empty()

    limiter_global = AdaptiveLimiter(min_interval=START_INTERVAL)

    results: List[Dict[str, Any]] = []
    started_at = time.time()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        fut_map = {ex.submit(process_one_cnpj, cnpj, limiter_global): cnpj for cnpj in uniq_inputs}
        done_count = 0
        for fut in as_completed(fut_map):
            row = fut.result()
            results.append(row)
            done_count += 1
            progress.progress(done_count / total)

            elapsed = time.time() - started_at
            remaining = total - done_count
            eff_rate = done_count / elapsed if elapsed > 0 else 0.0001
            eta_sec = remaining / eff_rate if eff_rate > 0 else 0
            finish_time = datetime.datetime.now(BRASILIA_TZ) + datetime.timedelta(seconds=int(eta_sec))
            eta_box.info(
                f"Progresso: **{done_count}/{total}**  •  Taxa efetiva: **{eff_rate:.2f} req/s**  •  "
                f"ETA: **{str(datetime.timedelta(seconds=int(eta_sec)))}** (≈ {finish_time.strftime('%H:%M:%S')})"
            )

    # Ordena pela ordem de entrada original
    order_index = {cnpj: i for i, cnpj in enumerate(uniq_inputs)}
    results.sort(key=lambda r: order_index.get(r.get("CNPJ", ""), 10**9))

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
