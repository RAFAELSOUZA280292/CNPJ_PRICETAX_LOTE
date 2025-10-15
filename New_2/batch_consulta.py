import streamlit as st
import requests
import re
import pandas as pd
import numpy as np
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
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# =========================
# Config da Aplicação
# =========================
st.set_page_config(
    page_title="Consulta de CNPJ em Lote - PriceTax (Turbo + Autosave robusto)",
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
URL_IBGE_MUNS      = "https://servicodados.ibge.gov.br/api/v1/localidades/estados/{uf}/municipios"
BRASILIA_TZ = ZoneInfo("America/Sao_Paulo")

MAX_WORKERS = 3
START_INTERVAL = 1.0
MIN_INTERVAL_FLOOR = 0.75
MIN_INTERVAL_CEIL  = 5.0
ADAPT_SUCC_WINDOW  = 18
ADAPT_FAIL_BACKOFF = 2.0
TOTAL_RETRIES = 3
REQ_TIMEOUT = 20
MAX_INPUTS = 1000
AUTOSAVE_BLOCK = 10
OUTPUT_DIR = "autosave_cnpj"

CSV_COLS = [
    "CNPJ_ORIGINAL","CNPJ_LIMPO","Razao Social","UF",
    "Municipio","Endereco",
    "Regime Tributario","Regime","Ano Regime Tributario",
    "Simples Nacional","MEI",
    "CNAE Principal","CNAE Secundario (primeiro)",
    "Codigo IBGE Municipio","TIMESTAMP"
]

_CACHE: Dict[str, Dict[str, Any]] = {}
_CACHE_LOCK = threading.Lock()

def cache_get(cnpj_key: str) -> Optional[Dict[str, Any]]:
    with _CACHE_LOCK:
        return _CACHE.get(cnpj_key)

def cache_set(cnpj_key: str, data: Dict[str, Any]) -> None:
    with _CACHE_LOCK:
        _CACHE[cnpj_key] = data

_thread_local = threading.local()
def get_session() -> requests.Session:
    s = getattr(_thread_local, "session", None)
    if s is None:
        s = requests.Session()
        adapter = HTTPAdapter(
            pool_connections=MAX_WORKERS * 2,
            pool_maxsize=MAX_WORKERS * 4,
            max_retries=Retry(total=0, backoff_factor=0, status_forcelist=[])
        )
        s.mount("http://", adapter)
        s.mount("https://", adapter)
        _thread_local.session = s
    return s

# =========================
# Helpers
# =========================
def limpar_cnpj(cnpj: str) -> str:
    return re.sub(r'[^0-9]', '', cnpj or "")

def calcular_digitos_verificadores_cnpj(cnpj_base_12_digitos: str) -> str:
    pesos_12 = [5,4,3,2,9,8,7,6,5,4,3,2]
    pesos_13 = [6,5,4,3,2,9,8,7,6,5,4,3,2]
    def dv(base, pesos):
        s = sum(int(base[i]) * pesos[i] for i in range(len(base)))
        r = s % 11
        return '0' if r < 2 else str(11 - r)
    d13 = dv(cnpj_base_12_digitos[:12], pesos_12)
    d14 = dv(cnpj_base_12_digitos[:12] + d13, pesos_13)
    return d13 + d14

def cnpj_is_valid(cnpj14: str) -> bool:
    if not cnpj14 or len(cnpj14) != 14 or len(set(cnpj14)) == 1:
        return False
    return cnpj14[-2:] == calcular_digitos_verificadores_cnpj(cnpj14[:12])

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
    latest = max(
        (r for r in regimes_list if isinstance(r, dict) and r.get('ano') is not None),
        key=lambda x: x['ano'], default=None
    )
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

# ==================================
#  Regras de Regime com prioridade MEI > Simples > NORMAL
# ==================================
def _col(df: pd.DataFrame, name: str) -> pd.Series:
    if name in df.columns:
        return df[name].astype(str)
    return pd.Series([""] * len(df), index=df.index, dtype=str)

def apply_regime_rules(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    s = _col(df, "Simples Nacional").str.upper().str.strip().fillna("")
    m = _col(df, "MEI").str.upper().str.strip().fillna("")
    df["Regime Tributario"] = np.where(
        m.eq("SIM"), "MEI",
        np.where(s.eq("SIM"), "Simples", "NORMAL")
    )
    return df

def migrate_old_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "Regime" not in df.columns:
        df["Regime"] = ""
    rt_old = _col(df, "Regime Tributario")
    mask_lucro = rt_old.str.contains(r"(?i)^lucro\s", na=False)
    mask_regime_vazio = _col(df, "Regime").str.strip().eq("") | _col(df, "Regime").isna()
    df.loc[mask_lucro & mask_regime_vazio, "Regime"] = rt_old[mask_lucro]
    df = apply_regime_rules(df)
    for col in CSV_COLS:
        if col not in df.columns:
            df[col] = ""
    df = df.reindex(columns=CSV_COLS)
    return df

# =========================
# Montagem de linha com prioridade MEI > Simples > NORMAL
# =========================
def montar_row(original_cnpj_str: str, cnpj_limpo: str,
               api_data: Optional[Dict[str, Any]], err_msg: Optional[str]) -> Dict[str, Any]:
    ts = datetime.datetime.now(BRASILIA_TZ).strftime("%Y-%m-%d %H:%M:%S")

    if api_data and "cnpj" in api_data:
        simples_flag = api_data.get('opcao_pelo_simples')
        mei_flag = api_data.get('opcao_pelo_mei')

        if mei_flag:
            regime_trib = "MEI"
        elif simples_flag:
            regime_trib = "Simples"
        else:
            regime_trib = "NORMAL"

        forma, ano = get_regime_tributario(api_data.get("regime_tributario", []))
        cnae_pri, cnae_sec = extrair_cnaes(api_data)

        endereco = " ".join(
            str(api_data.get(x, "")).strip()
            for x in ["logradouro", "numero", "complemento", "bairro"]
            if api_data.get(x)
        ).strip() or "N/A"

        municipio = api_data.get("municipio", "N/A")
        uf = api_data.get("uf", "N/A")

        ibge = api_data.get("municipio_ibge")
        if ibge in (None, "", "0"):
            ibge = "N/A"

        return {
            "CNPJ_ORIGINAL": original_cnpj_str,
            "CNPJ_LIMPO": cnpj_limpo,
            "Razao Social": api_data.get('razao_social', 'N/A'),
            "UF": uf,
            "Municipio": municipio,
            "Endereco": endereco,
            "Regime Tributario": regime_trib,
            "Regime": forma,
            "Ano Regime Tributario": ano,
            "Simples Nacional": "SIM" if simples_flag else ("NÃO" if simples_flag is False else "N/A"),
            "MEI": "SIM" if mei_flag else ("NÃO" if mei_flag is False else "N/A"),
            "CNAE Principal": cnae_pri,
            "CNAE Secundario (primeiro)": cnae_sec,
            "Codigo IBGE Municipio": str(ibge),
            "TIMESTAMP": ts
        }

    msg = err_msg or "Falha desconhecida"
    return {c: ("N/A" if c not in ["CNPJ_ORIGINAL","CNPJ_LIMPO","Razao Social"] else msg) for c in CSV_COLS}

# (demais blocos permanecem idênticos ao código anterior)
