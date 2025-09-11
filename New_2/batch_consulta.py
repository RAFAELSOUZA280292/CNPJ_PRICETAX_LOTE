import streamlit as st
import requests
import re
import pandas as pd
import time
import datetime
import io
import random
import threading
import hashlib
import os
import csv
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from zoneinfo import ZoneInfo
from typing import Dict, Tuple, Any, Optional, List, Set

# =========================
# Config da Aplicação
# =========================
st.set_page_config(
    page_title="Consulta de CNPJ em Lote - Adapta (Turbo + Autosave robusto)",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# =========================
# Tema
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
    code { color: #FFD700; }
</style>
""", unsafe_allow_html=True)

# =========================
# Constantes / Globais
# =========================
URL_BRASILAPI_CNPJ = "https://brasilapi.com.br/api/cnpj/v1/"
URL_IBGE_MUNS = "https://servicodados.ibge.gov.br/api/v1/localidades/estados/{uf}/municipios"
BRASILIA_TZ = ZoneInfo("America/Sao_Paulo")

# Parâmetros de desempenho (fixos)
MAX_WORKERS = 3
START_INTERVAL = 1.0

# Limitador adaptativo
MIN_INTERVAL_FLOOR = 0.75
MIN_INTERVAL_CEIL  = 5.0
ADAPT_SUCC_WINDOW  = 18
ADAPT_FAIL_BACKOFF = 2.0

# Robustez
TOTAL_RETRIES = 3
REQ_TIMEOUT   = 20

# Limites
MAX_INPUTS = 1000

# Autosave
AUTOSAVE_BLOCK = 10
OUTPUT_DIR = "autosave_cnpj"

CSV_COLS = [
    "CNPJ","Razao Social","Nome Fantasia","UF",
    "Simples Nacional","MEI","Regime Tributario","Ano Regime Tributario",
    "CNAE Principal","CNAE Secundario (primeiro)",
    "Endereco","Municipio","Codigo IBGE Municipio"
]

# ---------- Cache global (thread-safe) ----------
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
        if y in regimes_por_ano and regimes_por_ao[y]:
            return regimes_por_ao[y], str(y)
    latest = max((r for r in regimes_list if isinstance(r, dict) and r.get('ano') is not None),
                 key=lambda x: x['ano'], default=None)
    if latest:
        return latest.get('forma_de_tributacao', "N/A"), str(latest.get('ano', "N/A"))
    return "N/A", "N/A"

def extrair_cnaes(api_data: Dict[str, Any]) -> Tuple[str, str]:
    cnae_pri_cod = api_data.get("cnae_fiscal")
    cnae_pri_desc = api_data.get("cnae_fiscal_descricao")
    if cnae_pri_cod and cnae_pri_desc:
        cnae_principal = f"{cnae_pri_cod} - {cnae_pri_desc}"
    elif cnae_pri_cod:
        cnae_principal = str(cnae_pri_cod)
    else:
        cnae_principal = "N/A"
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

def humanize_seconds(seconds: float) -> str:
    s = int(max(0, round(seconds)))
    h, rem = divmod(s, 3600)
    m, s = divmod(rem, 60)
    parts = []
    if h: parts.append(f"{h}h")
    if m or h: parts.append(f"{m}m")
    parts.append(f"{s}s")
    return " ".join(parts)

def mk_job_id(cnpjs: List[str]) -> str:
    base = "\n".join([limpar_cnpj(x) for x in cnpjs])
    return hashlib.md5(base.encode("utf-8")).hexdigest()

def mk_paths(job_id: str) -> Tuple[str, str]:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    csv_path = os.path.join(OUTPUT_DIR, f"autosave_{job_id}.csv")
    xlsx_path = os.path.join(OUTPUT_DIR, f"resultado_{job_id}.xlsx")
    return csv_path, xlsx_path

def ensure_autosave_header(csv_path: str, expected_cols: List[str]) -> None:
    """Garante que o CSV tenha o cabeçalho esperado; migra se for diferente."""
    if not os.path.exists(csv_path):
        return
    try:
        with open(csv_path, "r", encoding="utf-8") as f:
            first_line = f.readline().strip()
        current_cols = [c.strip() for c in first_line.split(";")] if first_line else []
        if current_cols == expected_cols:
            return

        df_old = pd.read_csv(csv_path, sep=";", dtype=str, encoding="utf-8")
        for col in expected_cols:
            if col not in df_old.columns:
                df_old[col] = ""
        df_old = df_old[expected_cols]
        df_old.to_csv(csv_path, sep=";", index=False, encoding="utf-8")
    except Exception:
        base, ext = os.path.splitext(csv_path)
        try:
            os.rename(csv_path, base + "_backup_old_header" + ext)
        except Exception:
            pass

def load_done_set(csv_path: str) -> Set[str]:
    done: Set[str] = set()
    if os.path.exists(csv_path):
        try:
            df = pd.read_csv(csv_path, sep=";", dtype=str, encoding="utf-8")
            if "CNPJ" in df.columns:
                done.update([c.strip() for c in df["CNPJ"].dropna().astype(str).tolist()])
        except Exception:
            pass
    return done

# —— Escrita robusta com csv.DictWriter
def append_rows_csv(csv_path: str, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    file_exists = os.path.exists(csv_path)
    with open(csv_path, "a", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=CSV_COLS, delimiter=";", extrasaction="ignore")
        if not file_exists:
            w.writeheader()
        for r in rows:
            safe = {k: ("" if r.get(k) is None else str(r.get(k))) for k in CSV_COLS}
            w.writerow(safe)
    return len(rows)

# =========================
# Rate Limiter Adaptativo
# =========================
@dataclass
class AdaptiveLimiter:
    min_interval: float = START_INTERVAL
    last_request_ts: float = 0.0
    successes_since_last_adjust: int = 0
    def __post_init__(self):
        self._lock = threading.Lock()
    def wait_turn(self):
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
# Fallback IBGE (sem acento/caixa, cache por UF)
# =========================
_IBGE_CACHE: Dict[str, Dict[str, str]] = {}

def _norm_txt(s: str) -> str:
    """Remove acentos, baixa caixa e colapsa separadores para comparação."""
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = s.encode("ascii", "ignore").decode("ascii")  # remove acentos
    s = s.lower().strip()
    for ch in ["-", "–", "—", "/", "\\", ",", "."]:
        s = s.replace(ch, " ")
    s = " ".join(s.split())
    return s

def get_ibge_code_by_uf_city(uf: str, municipio: str) -> str:
    """Obtém o código IBGE via Localidades/IBGE quando BrasilAPI não traz `municipio_ibge`."""
    try:
        if not uf or not municipio:
            return "N/A"
        uf = uf.strip().upper()
        m_norm = _norm_txt(municipio)

        if uf not in _IBGE_CACHE:
            url = URL_IBGE_MUNS.format(uf=uf)
            r = requests.get(url, timeout=10)
            r.raise_for_status()
            _IBGE_CACHE[uf] = {_norm_txt(m["nome"]): str(m["id"]) for m in r.json()}

        code = _IBGE_CACHE[uf].get(m_norm)
        if code:
            return code

        # Fallback: começa/contém (para nomes compostos)
        for k, v in _IBGE_CACHE[uf].items():
            if m_norm.startswith(k) or k.startswith(m_norm):
                return v

        return "N/A"
    except Exception:
        return "N/A"

# =========================
# Requisição com retry/backoff
# =========================
def request_cnpj_with_retry(cnpj_query: str, limiter: AdaptiveLimiter) -> Tuple[Optional[Dict[str, Any]], str]:
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
            "Endereco": "N/A", "Municipio": "N/A", "Codigo IBGE Municipio": "N/A"
        }
    cached = cache_get(cleaned)
    if cached is not None:
        out = dict(cached); out["CNPJ"] = original_cnpj_str; return out

    cnpj_to_query = to_matriz_if_filial(cleaned)
    api_data, err_msg = request_cnpj_with_retry(cnpj_to_query, limiter)
    if api_data and "cnpj" in api_data:
        forma, ano = get_regime_tributario(api_data.get("regime_tributario", []))
        cnae_pri, cnae_sec = extrair_cnaes(api_data)

        endereco = " ".join(
            str(api_data.get(x, "")).strip()
            for x in ["logradouro", "numero", "complemento", "bairro"]
            if api_data.get(x)
        ).strip() or "N/A"

        municipio = api_data.get("municipio", "N/A")
        uf = api_data.get("uf", "N/A")

        ibge_code = api_data.get("municipio_ibge")
        if ibge_code in (None, "", "0"):
            ibge_code = get_ibge_code_by_uf_city(uf, municipio)

        row = {
            "CNPJ": original_cnpj_str,
            "Razao Social": api_data.get('razao_social', 'N/A'),
            "Nome Fantasia": api_data.get('nome_fantasia', 'N/A'),
            "UF": uf,
            "Simples Nacional": "SIM" if api_data.get('opcao_pelo_simples') else ("NÃO" if api_data.get('opcao_pelo_simples') is False else "N/A"),
            "MEI": "SIM" if api_data.get('opcao_pelo_mei') else ("NÃO" if api_data.get('opcao_pelo_mei') is False else "N/A"),
            "Regime Tributario": forma,
            "Ano Regime Tributario": ano,
            "CNAE Principal": cnae_pri,
            "CNAE Secundario (primeiro)": cnae_sec,
            "Endereco": endereco,
            "Municipio": municipio,
            "Codigo IBGE Municipio": str(ibge_code) if ibge_code else "N/A"
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
        "CNAE Principal": 'N/A', "CNAE Secundario (primeiro)": 'N/A',
        "Endereco": "N/A", "Municipio": "N/A", "Codigo IBGE Municipio": "N/A"
    }

# =========================
# UI
# =========================
st.markdown("<h1 style='text-align: center;'>Consulta de CNPJ em Lote (Turbo + Autosave robusto)</h1>", unsafe_allow_html=True)
st.markdown("<h3 style='text-align: center;'>Cole até 1.000 CNPJs (um por linha, vírgula, ponto e vírgula, ou espaço)</h3>", unsafe_allow_html=True)

cnpjs_input = st.text_area(
    "CNPJs (um por linha, ou separados por vírgula, ponto e vírgula ou espaço):",
    height=220,
    placeholder="Ex:\n00.000.000/0001-00\n11.111.111/1111-11\n22.222.222/2222-22"
)

if st.button("🔱 Consultar em Lote"):
    if not cnpjs_input.strip():
        st.warning("Por favor, insira os CNPJs para consultar."); st.stop()

    raw = [x for x in re.split(r'[\n,;\s]+', cnpjs_input.strip()) if x]
    uniq_inputs = list(dict.fromkeys(raw))
    if len(uniq_inputs) > MAX_INPUTS:
        st.error(f"Você enviou {len(uniq_inputs)} entradas. O limite deste app é {MAX_INPUTS}."); st.stop()

    job_id = mk_job_id(uniq_inputs)
    csv_autosave, xlsx_final = mk_paths(job_id)

    # Garante header correto (migra se necessário)
    ensure_autosave_header(csv_autosave, CSV_COLS)

    done_set = load_done_set(csv_autosave)
    to_do = [c for c in uniq_inputs if c not in done_set]

    st.info(
        f"**Autosave** ativo em: `{csv_autosave}`  \n"
        f"Já concluídos: **{len(done_set)}**  •  Restantes: **{len(to_do)}**"
    )

    all_rows_this_run: List[Dict[str, Any]] = []

    if to_do:
        st.write("---")
        st.write(f"Iniciando processamento de **{len(to_do)}** CNPJs pendentes…")
        progress = st.progress(0)
        status_box = st.empty()
        limiter_global = AdaptiveLimiter(min_interval=START_INTERVAL)

        started_at = time.time()
        buffer_rows: List[Dict[str, Any]] = []
        total_this_run = len(to_do)

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            fut_map = {ex.submit(process_one_cnpj, cnpj, limiter_global): cnpj for cnpj in to_do}
            processed_now = 0
            for fut in as_completed(fut_map):
                row = fut.result()
                buffer_rows.append(row)
                all_rows_this_run.append(row)
                processed_now += 1

                # Autosave por bloco
                if len(buffer_rows) >= AUTOSAVE_BLOCK:
                    written = append_rows_csv(csv_autosave, buffer_rows)
                    size_kb = os.path.getsize(csv_autosave) / 1024 if os.path.exists(csv_autosave) else 0
                    st.caption(f"🔸 Autosave: gravadas **{written}** linhas (arquivo ~{size_kb:.1f} KB).")
                    for r in buffer_rows:
                        done_set.add(r.get("CNPJ","").strip())
                    buffer_rows.clear()

                # Status amigável
                elapsed = time.time() - started_at
                remaining_now = total_this_run - processed_now
                eff_rate = processed_now / elapsed if elapsed > 0 else 0.0
                eta_sec = remaining_now / eff_rate if eff_rate > 0 else 0
                finish_time = datetime.datetime.now(BRASILIA_TZ) + datetime.timedelta(seconds=int(eta_sec))
                progress.progress(processed_now / total_this_run)
                status_box.info(
                    f"📊 **Andamento:** {processed_now} de {total_this_run} CNPJs (desta execução)  \n"
                    f"⚡ **Velocidade:** ~{eff_rate:.2f} CNPJs por segundo  \n"
                    f"⏳ **Tempo restante:** {humanize_seconds(eta_sec)}  \n"
                    f"🕒 **Previsão de término:** {finish_time.strftime('%H:%M:%S')}"
                )

        # flush final
        if buffer_rows:
            written = append_rows_csv(csv_autosave, buffer_rows)
            size_kb = os.path.getsize(csv_autosave) / 1024 if os.path.exists(csv_autosave) else 0
            st.caption(f"🔸 Autosave (final): gravadas **{written}** linhas (arquivo ~{size_kb:.1f} KB).")
            for r in buffer_rows:
                done_set.add(r.get("CNPJ","").strip())
            buffer_rows.clear()

        st.success(f"Concluído! Total geral no autosave: **{len(done_set)}** CNPJs.")

    # ===== Exibição/Download do consolidado =====
    st.markdown("---")
    st.subheader("Resultados (consolidados do autosave)")

    df_full = pd.DataFrame(columns=CSV_COLS)
    if os.path.exists(csv_autosave):
        try:
            df_full = pd.read_csv(csv_autosave, sep=";", dtype=str, encoding="utf-8")
        except Exception as e:
            st.warning(f"Não consegui ler o autosave agora ({e}). Vou mostrar o que foi obtido nesta execução.")
            df_full = pd.DataFrame(all_rows_this_run, columns=CSV_COLS)

    if df_full.empty and all_rows_this_run:
        df_full = pd.DataFrame(all_rows_this_run, columns=CSV_COLS)

    st.dataframe(df_full.fillna(""), use_container_width=True)

    if not df_full.empty:
        timestamp = datetime.datetime.now(BRASILIA_TZ).strftime("%Y%m%d_%H%M%S")
        excel_filename = f"CNPJ_Price_Tax_{timestamp}.xlsx"
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df_full.to_excel(writer, index=False, sheet_name='Resultados CNPJ')
        st.download_button(
            label="📥 Baixar Excel (consolidado)",
            data=output.getvalue(),
            file_name=excel_filename,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            help="Clique para baixar os resultados em .xlsx"
        )
