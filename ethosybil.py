#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import time
import json
import threading
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import httpx


# =========================================================
# CONFIG
# =========================================================
ALCHEMY_API_KEY = os.getenv("ALCHEMY_API_KEY", "YOUR_ALCHEMY_API_KEY_INPUT_HERE")
ANKR_API_KEY    = os.getenv("ANKR_API_KEY",    "YOUR_ANKR_API_KEY_INPUT_HERE")

TELEGRAM_TOKEN           = os.getenv("TELEGRAM_TOKEN", "INPUT_YOUR_TELEGRAM_API_TOKEN")
TELEGRAM_ALLOWED_CHAT_ID = os.getenv("TELEGRAM_ALLOWED_CHAT_ID", "")

ETHOS_API_BASE    = "https://api.ethos.network"
TELEGRAM_BASE_URL = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"

ETHOS_USERS_BY_ADDRESSES_URL      = f"{ETHOS_API_BASE}/api/v2/users/by/address"
ETHOS_SINGLE_USER_BY_ADDRESS_URL  = f"{ETHOS_API_BASE}/api/v2/user/by/address"

ETHOS_BULK_MAX_ADDRESSES = 500
ETHOS_RETRYABLE_STATUSES = {408, 429, 500, 502, 503, 504}

ALCHEMY_NETWORK_URLS: Dict[str, str] = {
    "ethereum": f"https://eth-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY}",
    "arbitrum": f"https://arb-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY}",
    "base":     f"https://base-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY}",
    "optimism": f"https://opt-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY}",
    "polygon":  f"https://polygon-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY}",
}

ANKR_ADVANCED_API_URL = f"https://rpc.ankr.com/multichain/{ANKR_API_KEY}"
ANKR_BSC_RPC_URL      = f"https://rpc.ankr.com/bsc/{ANKR_API_KEY}"

HTTP_TIMEOUT = httpx.Timeout(connect=12.0, read=45.0, write=30.0, pool=20.0)
POLL_TIMEOUT_SECONDS   = 30
SLEEP_ON_ERROR_SECONDS = 5

_POLL_HTTP_TIMEOUT = httpx.Timeout(
    connect=15.0,
    read=float(POLL_TIMEOUT_SECONDS + 20),
    write=10.0,
    pool=20.0,
)

MAX_RETRIES = 4
RETRY_DELAY = 2.0
MAX_BACKOFF = 20.0

# Fast mode defaults
DEFAULT_NETWORKS = ["base", "ethereum", "arbitrum", "optimism", "polygon"]
ENABLE_BSC = False
MAX_NETWORK_WORKERS = 5
PRE_SCORE_THRESHOLD = 3
MAX_PROMISING_CANDIDATES_PER_NETWORK = 40
ALCHEMY_MAX_PAGES = 8
CANDIDATE_WALLET_MAX_PAGES = 4
CANDIDATE_WALLET_MAX_TXS = 2000

# Optional slower stages (disabled by default for speed)
ENABLE_EXCHANGE_CLUSTER_SCAN = False
ENABLE_SHARED_EXCHANGE_SCAN = False
MAX_EXCHANGE_CONTRACTS_TO_SCAN  = 5
MAX_WALLETS_PER_EXCHANGE_SCAN   = 50
MAX_CLUSTER_WALLETS_FOR_EXCHANGE_CHECK = 20
SHARED_SCAN_WALLET_MAX_PAGES = 4
SHARED_SCAN_WALLET_MAX_TXS = 2500
HUGE_ADDRESS_TX_THRESHOLD = 10000
SKIP_DEEP_SCAN_FOR_HUGE_ADDRESS = True
CONTRACT_POPULARITY_EARLY_PAGES = 2
EARLY_POPULAR_SENDER_THRESHOLD = 100
EARLY_VERY_POPULAR_SENDER_THRESHOLD = 400

REPORT_MAX_DIRECT = 30
REPORT_MAX_CLUSTER = 30
REPORT_MAX_SHARED = 20
REPORT_MAX_WALLETS_PER_EXCHANGE = 5

TELEGRAM_MIN_SEND_INTERVAL = 1.1
TELEGRAM_MAX_RETRIES_429   = 5

# Scoring
FINAL_SCORE_THRESHOLD = 8
POSSIBLE_SCORE_THRESHOLD = 4

WEIGHT_DIRECT_ONCE = 4
WEIGHT_DIRECT_REPEAT = 4
WEIGHT_BIDIRECTIONAL = 3
WEIGHT_SHARED_RARE_CONTRACT = 3
WEIGHT_SHARED_MEDIUM_CONTRACT = 1
WEIGHT_SHARED_POPULAR_CONTRACT = -2
WEIGHT_SHARED_FUNDING_SOURCE = 5
WEIGHT_FIRST_FUNDER_SHARED = 6
WEIGHT_TIME_PROXIMITY_1H = 3
WEIGHT_TIME_PROXIMITY_24H = 1
WEIGHT_SIMILAR_AMOUNT = 2

POPULAR_CONTRACT_THRESHOLD = 100
VERY_POPULAR_CONTRACT_THRESHOLD = 1000
TIME_WINDOW_1H_SECONDS = 3600
TIME_WINDOW_24H_SECONDS = 86400
AMOUNT_SIMILARITY_REL_TOL = 0.10

_cancel_events: Dict[str, threading.Event] = {}
_active_threads: Dict[str, threading.Thread] = {}
_threads_lock = threading.Lock()

_tg_send_lock  = threading.Lock()
_tg_last_send: float = 0.0


class AnalysisCancelledError(Exception):
    pass


def _is_cancelled(chat_id: str) -> bool:
    ev = _cancel_events.get(chat_id)
    return ev is not None and ev.is_set()


def _check_cancelled(chat_id: str) -> None:
    if _is_cancelled(chat_id):
        raise AnalysisCancelledError("cancelled by user")


_RETRYABLE_EXCEPTIONS = (
    httpx.RemoteProtocolError,
    httpx.ConnectError,
    httpx.ReadError,
    httpx.WriteError,
    httpx.ReadTimeout,
    httpx.ConnectTimeout,
    httpx.PoolTimeout,
)


# =========================================================
# I18N
# =========================================================
_lang_store: Dict[str, str] = {}

_MESSAGES: Dict[str, Dict[str, str]] = {
    "help": {
        "ru": (
            "Бот готов.\n\n"
            "Отправь EVM-адрес кошелька:\n"
            "0x1234...abcd\n\n"
            "Команды:\n"
            "/lang ru — русский язык\n"
            "/lang en — English\n"
            "/cancel — остановить текущий скан\n\n"
            "Я:\n"
            "1) ищу прямые связи между кошельками\n"
            "2) считаю быстрый pre-score\n"
            "3) добираю историю только для promising-кандидатов\n"
            "4) проверяю shared funding + first funder\n"
            "5) проверяю сильных кандидатов в Ethos Network\n"
            "6) сканирую сети параллельно\n"
        ),
        "en": (
            "Bot is ready.\n\n"
            "Send an EVM wallet address:\n"
            "0x1234...abcd\n\n"
            "Commands:\n"
            "/lang ru — Russian\n"
            "/lang en — English\n"
            "/cancel — stop the current scan\n\n"
            "I will:\n"
            "1) find direct wallet links\n"
            "2) compute a quick pre-score\n"
            "3) fetch history only for promising candidates\n"
            "4) check shared funding + first funder\n"
            "5) check strong candidates in Ethos Network\n"
            "6) scan networks in parallel\n"
        ),
    },
    "invalid_address": {
        "ru": "Неверный адрес. Пришли EVM-адрес в формате 0x...",
        "en": "Invalid address. Please send an EVM address in 0x... format.",
    },
    "analyze_error": {
        "ru": "Ошибка анализа: {error}",
        "en": "Analysis error: {error}",
    },
    "lang_set_ru": {
        "ru": "Язык установлен: Русский 🇷🇺",
        "en": "Язык установлен: Русский 🇷🇺",
    },
    "lang_set_en": {
        "ru": "Language set: English 🇬🇧",
        "en": "Language set: English 🇬🇧",
    },
    "lang_unknown": {
        "ru": "Неизвестный язык. Используй /lang ru или /lang en",
        "en": "Unknown language. Use /lang ru or /lang en",
    },
    "start_analysis": {
        "ru": "Старт анализа: {target}\nСети: {networks}\nРежим: parallel + pre-score + first funder",
        "en": "Starting analysis: {target}\nNetworks: {networks}\nMode: parallel + pre-score + first funder",
    },
    "ethos_stage": {
        "ru": "Этап Ethos: проверяю главный адрес и {n} сильных адресов кластера...",
        "en": "Ethos stage: checking main address and {n} strong cluster addresses...",
    },
    "cancel_no_task": {
        "ru": "Нет активного скана для отмены.",
        "en": "No active scan to cancel.",
    },
    "cancel_requested": {
        "ru": "⛔ Отмена скана запрошена. Останавливаюсь после текущего шага...",
        "en": "⛔ Cancel requested. Stopping after the current step...",
    },
    "cancel_done": {
        "ru": "✅ Скан отменён.",
        "en": "✅ Scan cancelled.",
    },
    "already_running": {
        "ru": "⚠️ Скан уже запущен. Отправь /cancel чтобы остановить его перед новым запросом.",
        "en": "⚠️ A scan is already running. Send /cancel to stop it before starting a new one.",
    },
}


def get_lang(chat_id: str) -> str:
    return _lang_store.get(chat_id, "ru")


def set_lang(chat_id: str, lang: str) -> None:
    _lang_store[chat_id] = lang


def msg(key: str, lang: str = "ru", **kwargs: Any) -> str:
    translations = _MESSAGES.get(key) or {}
    text = translations.get(lang) or translations.get("ru") or key
    if kwargs:
        try:
            return text.format(**kwargs)
        except Exception:
            return text
    return text


# =========================================================
# HELPERS
# =========================================================
def log(message: str) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] {message}", flush=True)


ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"


def normalize_address(addr: Optional[str]) -> Optional[str]:
    if not addr or not isinstance(addr, str):
        return None
    addr = addr.strip()
    if not re.fullmatch(r"0x[a-fA-F0-9]{40}", addr):
        return None
    return addr.lower()


def split_text(text: str, limit: int = 3500) -> List[str]:
    if len(text) <= limit:
        return [text]
    chunks: List[str] = []
    current = ""
    for line in text.splitlines(True):
        if len(current) + len(line) > limit:
            if current:
                chunks.append(current)
            current = line
        else:
            current += line
    if current:
        chunks.append(current)
    return chunks


def ts_to_human(ts: str) -> str:
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    except Exception:
        return str(ts)


def safe_json(obj: Any) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False, indent=2)
    except Exception:
        return str(obj)


def exception_name(exc: BaseException) -> str:
    return exc.__class__.__name__


def backoff_sleep(attempt: int, base_delay: float = RETRY_DELAY, cap: float = MAX_BACKOFF) -> float:
    delay = min(base_delay * (2 ** max(0, attempt - 1)), cap)
    time.sleep(delay)
    return delay


def build_http_client() -> httpx.Client:
    limits = httpx.Limits(max_connections=100, max_keepalive_connections=25, keepalive_expiry=15.0)
    headers = {
        "User-Agent": "EthosSybilBotFast/4.0",
        "Accept": "application/json",
        "Connection": "close",
    }

    proxy_url = "http://Login:PASSWORD@IP:PORT"

    return httpx.Client(
        timeout=HTTP_TIMEOUT,
        headers=headers,
        limits=limits,
        http2=False,
        follow_redirects=True,
        trust_env=False,
        proxy=proxy_url,
    )


def http_request_with_retries(
    client: httpx.Client,
    method: str,
    url: str,
    *,
    op_name: str,
    max_retries: int = MAX_RETRIES,
    retry_on_statuses: Optional[Set[int]] = None,
    **kwargs: Any,
) -> Dict[str, Any]:
    retry_on_statuses = retry_on_statuses or {408, 429, 500, 502, 503, 504}
    last_error: Optional[str] = None
    last_status: Optional[int] = None

    for attempt in range(1, max_retries + 1):
        try:
            resp = client.request(method, url, **kwargs)
            last_status = resp.status_code

            if resp.status_code in retry_on_statuses and attempt < max_retries:
                log(f"[http] {op_name} retryable status={resp.status_code} attempt={attempt}/{max_retries}")
                backoff_sleep(attempt)
                continue

            return {"ok": True, "response": resp, "status_code": resp.status_code}

        except _RETRYABLE_EXCEPTIONS as e:
            last_error = f"{exception_name(e)}: {e}"
            if attempt < max_retries:
                delay = min(RETRY_DELAY * (2 ** (attempt - 1)), MAX_BACKOFF)
                log(f"[http] {op_name} conn error attempt={attempt}/{max_retries}: {last_error}; retry in {delay:.0f}s")
                time.sleep(delay)
                continue
            break

        except httpx.TimeoutException as e:
            last_error = f"{exception_name(e)}: {e}"
            if attempt < max_retries:
                delay = min(RETRY_DELAY * (2 ** (attempt - 1)), MAX_BACKOFF)
                log(f"[http] {op_name} timeout attempt={attempt}/{max_retries}: {last_error}; retry in {delay:.0f}s")
                time.sleep(delay)
                continue
            break

        except httpx.HTTPError as e:
            last_error = f"{exception_name(e)}: {e}"
            break

        except Exception as e:
            last_error = f"{exception_name(e)}: {e}"
            break

    return {"ok": False, "error": last_error or "unknown_error", "status_code": last_status, "response": None}


# =========================================================
# TELEGRAM
# =========================================================
def telegram_post(client: httpx.Client, method: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    result = http_request_with_retries(
        client, "POST",
        f"{TELEGRAM_BASE_URL}/{method}",
        op_name=f"telegram:{method}",
        json=payload,
        max_retries=3,
    )
    if not result["ok"]:
        log(f"[telegram] {method} failed: {result.get('error')}")
        return {"ok": False, "data": None}
    try:
        return {"ok": True, "data": result["response"].json()}
    except Exception as e:
        log(f"[telegram] {method} invalid json: {e}")
        return {"ok": False, "data": None}


def telegram_get(client: httpx.Client, method: str, params: Dict[str, Any]) -> Dict[str, Any]:
    result = http_request_with_retries(
        client, "GET",
        f"{TELEGRAM_BASE_URL}/{method}",
        op_name=f"telegram:{method}",
        params=params,
        max_retries=3,
    )
    if not result["ok"]:
        log(f"[telegram] {method} failed: {result.get('error')}")
        return {"ok": False, "data": None}
    try:
        return {"ok": True, "data": result["response"].json()}
    except Exception as e:
        log(f"[telegram] {method} invalid json: {e}")
        return {"ok": False, "data": None}


def send_telegram_message(client: httpx.Client, chat_id: str, text: str) -> None:
    global _tg_last_send

    for chunk in split_text(text):
        while True:
            with _tg_send_lock:
                now = time.monotonic()
                wait = TELEGRAM_MIN_SEND_INTERVAL - (now - _tg_last_send)
                if wait <= 0:
                    _tg_last_send = now
                    break
            time.sleep(max(wait, 0.05))

        payload = {"chat_id": chat_id, "text": chunk, "disable_web_page_preview": True}

        for attempt in range(1, TELEGRAM_MAX_RETRIES_429 + 1):
            result = http_request_with_retries(
                client, "POST",
                f"{TELEGRAM_BASE_URL}/sendMessage",
                op_name="telegram:sendMessage",
                json=payload,
                max_retries=1,
                retry_on_statuses=set(),
            )

            resp = result.get("response")
            if result["ok"] and resp is not None and resp.status_code == 200:
                break

            if resp is not None and resp.status_code == 429:
                retry_after = 5
                try:
                    body = resp.json()
                    retry_after = int(body.get("parameters", {}).get("retry_after", 5))
                except Exception:
                    pass
                log(f"[telegram] 429 rate limit, sleeping {retry_after}s (attempt {attempt}/{TELEGRAM_MAX_RETRIES_429})")
                time.sleep(retry_after + 0.5)
                with _tg_send_lock:
                    _tg_last_send = time.monotonic()
                continue

            log(f"[telegram] sendMessage failed status={resp.status_code if resp else 'no_resp'}: {result.get('error')}")
            break


def get_updates(client: httpx.Client, offset: Optional[int]) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {
        "timeout": POLL_TIMEOUT_SECONDS,
        "allowed_updates": json.dumps(["message"]),
    }
    if offset is not None:
        params["offset"] = offset

    url = f"{TELEGRAM_BASE_URL}/getUpdates"
    try:
        resp = client.get(url, params=params, timeout=_POLL_HTTP_TIMEOUT)
    except (httpx.ReadTimeout, httpx.PoolTimeout):
        return []
    except httpx.ConnectTimeout:
        log("[telegram] getUpdates connect timeout, retrying next cycle")
        return []
    except _RETRYABLE_EXCEPTIONS as e:
        log(f"[telegram] getUpdates network error: {exception_name(e)}: {e}")
        return []
    except Exception as e:
        log(f"[telegram] getUpdates unexpected error: {exception_name(e)}: {e}")
        return []

    if resp.status_code != 200:
        log(f"[telegram] getUpdates status={resp.status_code}")
        return []
    try:
        data = resp.json()
    except Exception:
        return []
    if not data.get("ok"):
        return []
    return data.get("result", [])


# =========================================================
# RPC HELPERS
# =========================================================
def alchemy_rpc(client: httpx.Client, network: str, method: str, params: List[Any]) -> Dict[str, Any]:
    url = ALCHEMY_NETWORK_URLS.get(network)
    if not url or not ALCHEMY_API_KEY or ALCHEMY_API_KEY.startswith("PASTE_"):
        return {"ok": False, "error": "alchemy_not_configured", "data": None}

    payload = {"id": 1, "jsonrpc": "2.0", "method": method, "params": params}
    result = http_request_with_retries(
        client,
        "POST",
        url,
        op_name=f"alchemy:{network}:{method}",
        json=payload,
        max_retries=MAX_RETRIES,
    )
    if not result["ok"]:
        return {"ok": False, "error": result.get("error"), "data": None}

    try:
        data = result["response"].json()
    except Exception as e:
        return {"ok": False, "error": f"invalid_json: {e}", "data": None}

    if "error" in data:
        return {"ok": False, "error": safe_json(data["error"]), "data": data}

    return {"ok": True, "data": data}


def ankr_advanced_post(client: httpx.Client, method: str, params: Dict[str, Any]) -> Dict[str, Any]:
    if not ANKR_API_KEY or ANKR_API_KEY.startswith("PASTE_"):
        return {"ok": False, "error": "ankr_not_configured", "data": None}

    payload = {"id": 1, "jsonrpc": "2.0", "method": method, "params": params}
    result = http_request_with_retries(
        client,
        "POST",
        ANKR_ADVANCED_API_URL,
        op_name=f"ankr:{method}",
        json=payload,
        max_retries=MAX_RETRIES,
    )
    if not result["ok"]:
        return {"ok": False, "error": result.get("error"), "data": None}

    try:
        data = result["response"].json()
    except Exception as e:
        return {"ok": False, "error": f"invalid_json: {e}", "data": None}

    if "error" in data:
        return {"ok": False, "error": safe_json(data["error"]), "data": data}

    return {"ok": True, "data": data}


def alchemy_get_code(client: httpx.Client, network: str, address: str) -> Optional[str]:
    result = alchemy_rpc(client, network, "eth_getCode", [address, "latest"])
    if not result["ok"]:
        return None
    return (result.get("data") or {}).get("result")


def ankr_bsc_get_code(client: httpx.Client, address: str) -> Optional[str]:
    payload = {"id": 1, "jsonrpc": "2.0", "method": "eth_getCode", "params": [address, "latest"]}
    result = http_request_with_retries(
        client,
        "POST",
        ANKR_BSC_RPC_URL,
        op_name=f"ankr:bsc:eth_getCode:{address}",
        json=payload,
        max_retries=MAX_RETRIES,
    )
    if not result["ok"]:
        return None
    try:
        data = result["response"].json()
    except Exception:
        return None
    if "error" in data:
        return None
    return data.get("result")


def get_normal_transactions_alchemy(
    client: httpx.Client,
    address: str,
    network: str,
    *,
    max_pages_override: Optional[int] = None,
    max_total_rows: Optional[int] = None,
) -> List[Dict[str, Any]]:
    if network not in ALCHEMY_NETWORK_URLS:
        return []

    max_pages = max_pages_override if max_pages_override is not None else ALCHEMY_MAX_PAGES

    def fetch_direction(direction_params: Dict[str, Any]) -> List[Dict[str, Any]]:
        all_rows: List[Dict[str, Any]] = []
        page_key: Optional[str] = None
        pages = 0

        while pages < max_pages:
            if max_total_rows is not None and len(all_rows) >= max_total_rows:
                break

            params_obj: Dict[str, Any] = {
                "fromBlock": "0x0",
                "toBlock": "latest",
                "category": ["external"],
                "withMetadata": True,
                "excludeZeroValue": False,
                "maxCount": hex(1000),
                **direction_params,
            }
            if page_key:
                params_obj["pageKey"] = page_key

            result = alchemy_rpc(client, network, "alchemy_getAssetTransfers", [params_obj])
            if not result["ok"]:
                break

            payload = (result.get("data") or {}).get("result", {})
            transfers = payload.get("transfers", [])
            if not isinstance(transfers, list) or not transfers:
                break

            if max_total_rows is not None:
                remaining = max_total_rows - len(all_rows)
                if remaining <= 0:
                    break
                all_rows.extend(transfers[:remaining])
            else:
                all_rows.extend(transfers)

            page_key = payload.get("pageKey")
            pages += 1
            if not page_key:
                break

        return all_rows

    outgoing = fetch_direction({"fromAddress": address})
    incoming = fetch_direction({"toAddress": address})

    rows: List[Dict[str, Any]] = []
    seen: Set[str] = set()

    def normalize_transfer(t: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        tx_hash = t.get("hash")
        unique_key = tx_hash or f"{t.get('from')}:{t.get('to')}:{t.get('category')}:{t.get('metadata')}"
        if unique_key in seen:
            return None
        seen.add(unique_key)

        from_addr = normalize_address(t.get("from"))
        to_addr   = normalize_address(t.get("to"))
        if not from_addr or not to_addr:
            return None

        metadata     = t.get("metadata") or {}
        block_num    = t.get("blockNum")
        raw_contract = t.get("rawContract") or {}
        value        = raw_contract.get("value")

        ts = "0"
        timestamp_str = metadata.get("blockTimestamp")
        if timestamp_str:
            try:
                dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
                ts = str(int(dt.timestamp()))
            except Exception:
                ts = "0"

        return {
            "hash":        tx_hash or unique_key,
            "from":        from_addr,
            "to":          to_addr,
            "timeStamp":   ts,
            "value":       str(value or "0"),
            "blockNumber": str(block_num or ""),
        }

    for item in outgoing + incoming:
        row = normalize_transfer(item)
        if row:
            rows.append(row)

    rows.sort(key=lambda x: int(x.get("timeStamp", "0") or "0"), reverse=True)
    return rows


def get_normal_transactions_ankr_bsc(
    client: httpx.Client,
    address: str,
    *,
    max_pages_override: Optional[int] = None,
    max_total_rows: Optional[int] = None,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    seen: Set[str] = set()
    page_token: Optional[str] = None
    pages = 0
    max_pages = max_pages_override if max_pages_override is not None else ALCHEMY_MAX_PAGES

    while pages < max_pages:
        if max_total_rows is not None and len(rows) >= max_total_rows:
            break

        params: Dict[str, Any] = {
            "blockchain": "bsc",
            "address": address,
            "pageSize": 10000,
            "descOrder": True,
        }
        if page_token:
            params["pageToken"] = page_token

        result = ankr_advanced_post(client, "ankr_getTransactionsByAddress", params)
        if not result["ok"]:
            break

        data = (result.get("data") or {}).get("result") or {}
        transactions = data.get("transactions") or []
        if not isinstance(transactions, list) or not transactions:
            break

        for tx in transactions:
            if max_total_rows is not None and len(rows) >= max_total_rows:
                break

            tx_hash = tx.get("hash")
            if not tx_hash or tx_hash in seen:
                continue
            seen.add(tx_hash)

            from_addr = normalize_address(tx.get("from"))
            to_addr = normalize_address(tx.get("to"))
            if not from_addr or not to_addr:
                continue

            ts_raw = tx.get("timestamp")
            try:
                ts = str(int(ts_raw)) if ts_raw is not None else "0"
            except Exception:
                ts = "0"

            value_hex = tx.get("value", "0x0")
            try:
                value = str(int(value_hex, 16)) if isinstance(value_hex, str) and value_hex.startswith("0x") else str(value_hex or "0")
            except Exception:
                value = "0"

            block_num = tx.get("blockNumber", "")
            try:
                block_num = str(int(block_num, 16)) if isinstance(block_num, str) and block_num.startswith("0x") else str(block_num or "")
            except Exception:
                block_num = str(block_num or "")

            rows.append({
                "hash": tx_hash,
                "from": from_addr,
                "to": to_addr,
                "timeStamp": ts,
                "value": value,
                "blockNumber": block_num,
            })

        page_token = data.get("nextPageToken")
        pages += 1
        if not page_token:
            break

    return rows


def get_normal_transactions(
    client: httpx.Client,
    address: str,
    network: str,
    *,
    max_pages_override: Optional[int] = None,
    max_total_rows: Optional[int] = None,
) -> List[Dict[str, Any]]:
    if network == "bsc":
        return get_normal_transactions_ankr_bsc(
            client,
            address,
            max_pages_override=max_pages_override,
            max_total_rows=max_total_rows,
        )
    return get_normal_transactions_alchemy(
        client,
        address,
        network,
        max_pages_override=max_pages_override,
        max_total_rows=max_total_rows,
    )


def is_contract_address(
    client: httpx.Client,
    address: str,
    network: str,
    cache: Dict[Tuple[str, str], bool],
) -> bool:
    key = (network, address)
    cached = cache.get(key)
    if cached is not None:
        return cached

    code: Optional[str] = None
    if network == "bsc":
        code = ankr_bsc_get_code(client, address)
    elif network in ALCHEMY_NETWORK_URLS:
        code = alchemy_get_code(client, network, address)

    is_contract = isinstance(code, str) and code not in ("0x", "0x0")
    cache[key] = is_contract
    return is_contract


def batch_populate_contract_cache(
    client: httpx.Client,
    network: str,
    addresses: List[str],
    cache: Dict[Tuple[str, str], bool],
    batch_size: int = 200,
) -> None:
    to_check = [a for a in dict.fromkeys(addresses) if (network, a) not in cache]
    if not to_check:
        return

    if network == "bsc":
        rpc_url = ANKR_BSC_RPC_URL
        provider = "ankr:bsc"
    elif network in ALCHEMY_NETWORK_URLS:
        rpc_url = ALCHEMY_NETWORK_URLS[network]
        provider = f"alchemy:{network}"
    else:
        for addr in to_check:
            cache[(network, addr)] = False
        return

    for chunk_start in range(0, len(to_check), batch_size):
        chunk = to_check[chunk_start: chunk_start + batch_size]
        batch_payload = [
            {"id": idx, "jsonrpc": "2.0", "method": "eth_getCode", "params": [addr, "latest"]}
            for idx, addr in enumerate(chunk)
        ]

        result = http_request_with_retries(
            client,
            "POST",
            rpc_url,
            op_name=f"{provider}:batch_eth_getCode[{len(chunk)}]",
            json=batch_payload,
            max_retries=MAX_RETRIES,
        )
        if not result["ok"]:
            for addr in chunk:
                cache[(network, addr)] = False
            continue

        try:
            responses = result["response"].json()
        except Exception:
            for addr in chunk:
                cache[(network, addr)] = False
            continue

        if not isinstance(responses, list):
            responses = [responses]

        id_to_code: Dict[int, str] = {}
        for resp in responses:
            if isinstance(resp, dict):
                rid = resp.get("id")
                code = resp.get("result") or "0x"
                if rid is not None:
                    id_to_code[int(rid)] = code

        for idx, addr in enumerate(chunk):
            code = id_to_code.get(idx, "0x")
            cache[(network, addr)] = isinstance(code, str) and code not in ("0x", "0x0")


# =========================================================
# SCORING
# =========================================================
def new_candidate(address: str) -> Dict[str, Any]:
    return {
        "address": address,
        "score": 0,
        "pre_score": 0,
        "confidence": "low",
        "signals": [],
        "signal_types": set(),
        "direct_count": 0,
        "incoming_count": 0,
        "outgoing_count": 0,
        "bidirectional": False,
        "repeat_transfer": False,
        "shared_contracts": [],
        "shared_funding_sources": [],
        "shared_first_funder": None,
        "time_proximity_hits": 0,
        "similar_amount_hits": 0,
        "first_seen_ts": None,
        "last_seen_ts": None,
    }


def add_signal(candidate: Dict[str, Any], signal_type: str, score: int, details: str, *, pre_only: bool = False) -> None:
    candidate["score"] += score
    if pre_only:
        candidate["pre_score"] += score
    candidate["signals"].append({"type": signal_type, "score": score, "details": details})
    candidate["signal_types"].add(signal_type)


def finalize_candidate_confidence(candidate: Dict[str, Any]) -> None:
    score = candidate["score"]
    signal_count = len(candidate["signal_types"])

    if score >= FINAL_SCORE_THRESHOLD and signal_count >= 2:
        candidate["confidence"] = "high"
    elif score >= POSSIBLE_SCORE_THRESHOLD:
        candidate["confidence"] = "medium"
    else:
        candidate["confidence"] = "low"


def classify_contract_popularity(unique_senders_count: int) -> str:
    if unique_senders_count >= VERY_POPULAR_CONTRACT_THRESHOLD:
        return "very_popular"
    if unique_senders_count >= POPULAR_CONTRACT_THRESHOLD:
        return "popular"
    if unique_senders_count >= 10:
        return "medium"
    return "rare"


def contract_popularity_score(popularity: str) -> int:
    if popularity == "rare":
        return WEIGHT_SHARED_RARE_CONTRACT
    if popularity == "medium":
        return WEIGHT_SHARED_MEDIUM_CONTRACT
    return WEIGHT_SHARED_POPULAR_CONTRACT


def should_skip_deep_contract_scan(contract_meta: Dict[str, Any]) -> bool:
    popularity = contract_meta.get("popularity", "medium")
    return popularity in ("popular", "very_popular")


def estimate_time_proximity(target: str, candidate: str, target_txs: List[Dict[str, Any]], candidate_txs: List[Dict[str, Any]]) -> Dict[str, int]:
    def outgoing_timestamps(wallet: str, txs: List[Dict[str, Any]]) -> List[int]:
        out = []
        for tx in txs:
            from_addr = normalize_address(tx.get("from"))
            if from_addr != wallet:
                continue
            try:
                out.append(int(tx.get("timeStamp", "0") or "0"))
            except Exception:
                pass
        return sorted(x for x in out if x > 0)

    t1 = outgoing_timestamps(target, target_txs)
    t2 = outgoing_timestamps(candidate, candidate_txs)

    hits_1h = 0
    hits_24h = 0
    j = 0
    for x in t1:
        while j < len(t2) and t2[j] < x - TIME_WINDOW_24H_SECONDS:
            j += 1
        k = j
        while k < len(t2) and t2[k] <= x + TIME_WINDOW_24H_SECONDS:
            diff = abs(t2[k] - x)
            if diff <= TIME_WINDOW_1H_SECONDS:
                hits_1h += 1
            else:
                hits_24h += 1
            k += 1
    return {"hits_1h": hits_1h, "hits_24h": hits_24h}


def estimate_amount_similarity(target: str, candidate: str, target_txs: List[Dict[str, Any]], candidate_txs: List[Dict[str, Any]]) -> int:
    def outgoing_values(wallet: str, txs: List[Dict[str, Any]]) -> List[int]:
        vals = []
        for tx in txs:
            from_addr = normalize_address(tx.get("from"))
            if from_addr != wallet:
                continue
            try:
                v = int(tx.get("value", "0") or "0")
                if v > 0:
                    vals.append(v)
            except Exception:
                pass
        return vals

    a = outgoing_values(target, target_txs)
    b = outgoing_values(candidate, candidate_txs)

    hits = 0
    for x in a[:50]:
        for y in b[:50]:
            if x == 0 or y == 0:
                continue
            rel = abs(x - y) / max(x, y)
            if rel <= AMOUNT_SIMILARITY_REL_TOL:
                hits += 1
                break
    return hits


def build_pre_candidate_scores(
    direct_wallets: Dict[str, Dict[str, Any]],
    exchange_cluster_wallets: Dict[str, Dict[str, Any]],
    contract_popularity_map: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Dict[str, Dict[str, Any]]:
    contract_popularity_map = contract_popularity_map or {}
    candidates: Dict[str, Dict[str, Any]] = {}

    def ensure(addr: str) -> Dict[str, Any]:
        if addr not in candidates:
            candidates[addr] = new_candidate(addr)
        return candidates[addr]

    for addr, info in direct_wallets.items():
        c = ensure(addr)
        c["direct_count"] = info["count"]
        c["incoming_count"] = info["incoming_count"]
        c["outgoing_count"] = info["outgoing_count"]
        c["repeat_transfer"] = info["repeat_transfer"]
        c["bidirectional"] = info.get("bidirectional", False)
        c["first_seen_ts"] = info["timestamps"][-1] if info.get("timestamps") else None
        c["last_seen_ts"] = info["timestamps"][0] if info.get("timestamps") else None

        add_signal(c, "direct_transfer", WEIGHT_DIRECT_ONCE, f"count={info['count']}", pre_only=True)
        if info["repeat_transfer"]:
            add_signal(c, "repeat_transfer", WEIGHT_DIRECT_REPEAT, "count>1", pre_only=True)
        if info.get("bidirectional"):
            add_signal(c, "bidirectional", WEIGHT_BIDIRECTIONAL, "in>0 and out>0", pre_only=True)

    for addr, info in exchange_cluster_wallets.items():
        c = ensure(addr)
        for contract_addr in info.get("shared_exchange_addresses", []):
            meta = contract_popularity_map.get(contract_addr, {})
            popularity = meta.get("popularity", "medium")
            score = contract_popularity_score(popularity)
            c["shared_contracts"].append(contract_addr)
            add_signal(c, "shared_contract", score, f"{contract_addr} popularity={popularity}", pre_only=True)

    for c in candidates.values():
        finalize_candidate_confidence(c)

    return candidates


def split_candidates_by_confidence(candidates: Dict[str, Dict[str, Any]]) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    final_cluster: Dict[str, Dict[str, Any]] = {}
    possible_cluster: Dict[str, Dict[str, Any]] = {}

    for addr, c in candidates.items():
        if c["score"] >= FINAL_SCORE_THRESHOLD and len(c["signal_types"]) >= 2:
            final_cluster[addr] = c
        elif c["score"] >= POSSIBLE_SCORE_THRESHOLD:
            possible_cluster[addr] = c

    return final_cluster, possible_cluster


# =========================================================
# ANALYSIS HELPERS
# =========================================================
def analyze_direct_wallet_transfers(
    client: httpx.Client,
    target: str,
    network: str,
    txs: List[Dict[str, Any]],
    contract_cache: Dict[Tuple[str, str], bool],
) -> Dict[str, Dict[str, Any]]:
    dedup_seen: Set[str] = set()
    pending: List[Dict[str, Any]] = []

    for tx in txs:
        from_addr = normalize_address(tx.get("from"))
        to_addr = normalize_address(tx.get("to"))
        if not from_addr or not to_addr:
            continue
        if target not in (from_addr, to_addr):
            continue

        tx_hash = tx.get("hash")
        ts = str(tx.get("timeStamp", ""))
        value_wei = tx.get("value", "0")
        dedup_key = tx_hash or f"{from_addr}:{to_addr}:{ts}:{value_wei}"
        if dedup_key in dedup_seen:
            continue
        dedup_seen.add(dedup_key)

        counterparty = to_addr if from_addr == target else from_addr
        if counterparty and counterparty != target:
            pending.append({
                "from": from_addr,
                "to": to_addr,
                "hash": tx_hash,
                "ts": ts,
                "counterparty": counterparty,
                "value": str(value_wei or "0"),
            })

    unique_counterparties = list(dict.fromkeys(r["counterparty"] for r in pending))
    if unique_counterparties:
        log(f"[{network.capitalize()}] Checking {len(unique_counterparties)} addresses for contract status...")
        batch_populate_contract_cache(client, network, unique_counterparties, contract_cache)

    result: Dict[str, Dict[str, Any]] = {}
    for rec in pending:
        counterparty = rec["counterparty"]
        if contract_cache.get((network, counterparty), False):
            continue

        direction = "out" if rec["from"] == target else "in"
        bucket = result.setdefault(
            counterparty,
            {
                "address": counterparty,
                "count": 0,
                "incoming_count": 0,
                "outgoing_count": 0,
                "timestamps": [],
                "tx_hashes": [],
                "repeat_transfer": False,
                "bidirectional": False,
                "values": [],
            },
        )
        bucket["count"] += 1
        if direction == "in":
            bucket["incoming_count"] += 1
        else:
            bucket["outgoing_count"] += 1

        if rec["ts"]:
            bucket["timestamps"].append(rec["ts"])
        if rec["hash"]:
            bucket["tx_hashes"].append(rec["hash"])
        bucket["values"].append(rec["value"])

    for info in result.values():
        info["repeat_transfer"] = info["count"] > 1
        info["bidirectional"] = info["incoming_count"] > 0 and info["outgoing_count"] > 0

    return result


def get_exchange_addresses_from_txs(
    target: str,
    network: str,
    txs: List[Dict[str, Any]],
    contract_cache: Dict[Tuple[str, str], bool],
) -> Dict[str, Dict[str, Any]]:
    result: Dict[str, Dict[str, Any]] = {}
    for tx in txs:
        from_addr = normalize_address(tx.get("from"))
        to_addr = normalize_address(tx.get("to"))
        if from_addr != target or not to_addr or to_addr == target:
            continue
        if to_addr == ZERO_ADDRESS:
            continue
        try:
            value = int(tx.get("value", "0") or "0")
        except (ValueError, TypeError):
            value = 0
        if value == 0:
            continue
        if contract_cache.get((network, to_addr), False) and to_addr not in result:
            result[to_addr] = {"exchange_address": to_addr, "label": "Contract"}
    return result


def estimate_contract_popularity_early(client: httpx.Client, network: str, contract_addr: str) -> Dict[str, Any]:
    unique_senders: Set[str] = set()
    tx_count_seen = 0
    early_stopped = False

    if network == "bsc":
        page_token: Optional[str] = None
        pages = 0
        while pages < CONTRACT_POPULARITY_EARLY_PAGES:
            params: Dict[str, Any] = {
                "blockchain": "bsc",
                "address": contract_addr,
                "pageSize": 10000,
                "descOrder": True,
            }
            if page_token:
                params["pageToken"] = page_token

            result = ankr_advanced_post(client, "ankr_getTransactionsByAddress", params)
            if not result["ok"]:
                break

            data = (result.get("data") or {}).get("result") or {}
            transactions = data.get("transactions") or []
            if not isinstance(transactions, list) or not transactions:
                break

            tx_count_seen += len(transactions)
            for tx in transactions:
                from_addr = normalize_address(tx.get("from"))
                to_addr = normalize_address(tx.get("to"))
                if to_addr == contract_addr and from_addr:
                    unique_senders.add(from_addr)
            if len(unique_senders) >= EARLY_POPULAR_SENDER_THRESHOLD:
                early_stopped = True
                break
            page_token = data.get("nextPageToken")
            pages += 1
            if not page_token:
                break
    else:
        page_key: Optional[str] = None
        pages = 0
        while pages < CONTRACT_POPULARITY_EARLY_PAGES:
            params_obj: Dict[str, Any] = {
                "fromBlock": "0x0",
                "toBlock": "latest",
                "category": ["external"],
                "withMetadata": False,
                "excludeZeroValue": False,
                "maxCount": hex(1000),
                "toAddress": contract_addr,
            }
            if page_key:
                params_obj["pageKey"] = page_key

            result = alchemy_rpc(client, network, "alchemy_getAssetTransfers", [params_obj])
            if not result["ok"]:
                break

            payload = (result.get("data") or {}).get("result", {})
            transfers = payload.get("transfers", [])
            if not isinstance(transfers, list) or not transfers:
                break

            tx_count_seen += len(transfers)
            for tx in transfers:
                from_addr = normalize_address(tx.get("from"))
                to_addr = normalize_address(tx.get("to"))
                if to_addr == contract_addr and from_addr:
                    unique_senders.add(from_addr)
            if len(unique_senders) >= EARLY_POPULAR_SENDER_THRESHOLD:
                early_stopped = True
                break
            page_key = payload.get("pageKey")
            pages += 1
            if not page_key:
                break

    count = len(unique_senders)
    popularity = classify_contract_popularity(count)
    if count >= EARLY_VERY_POPULAR_SENDER_THRESHOLD:
        popularity = "very_popular"
    elif count >= EARLY_POPULAR_SENDER_THRESHOLD and popularity in ("rare", "medium"):
        popularity = "popular"

    return {
        "exchange_address": contract_addr,
        "unique_senders_count": count,
        "popularity": popularity,
        "early_stopped": early_stopped,
        "tx_count_seen": tx_count_seen,
    }


def build_contract_popularity_map(
    client: httpx.Client,
    network: str,
    exchange_addresses: Dict[str, Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    result: Dict[str, Dict[str, Any]] = {}
    limited = dict(list(exchange_addresses.items())[:MAX_EXCHANGE_CONTRACTS_TO_SCAN])
    for contract_addr in limited:
        meta = estimate_contract_popularity_early(client, network, contract_addr)
        result[contract_addr] = {
            "unique_senders_count": meta["unique_senders_count"],
            "popularity": meta["popularity"],
            "early_stopped": meta["early_stopped"],
            "tx_count_seen": meta["tx_count_seen"],
        }
    return result


def find_wallets_sharing_exchanges(
    client: httpx.Client,
    target: str,
    network: str,
    exchange_addresses: Dict[str, Dict[str, Any]],
    contract_cache: Dict[Tuple[str, str], bool],
    contract_popularity_map: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Dict[str, Dict[str, Any]]:
    candidates: Dict[str, List[Tuple[str, str]]] = {}
    limited = dict(list(exchange_addresses.items())[:MAX_EXCHANGE_CONTRACTS_TO_SCAN])

    for ex_addr, ex_meta in limited.items():
        popularity_meta = (contract_popularity_map or {}).get(ex_addr, {})
        if should_skip_deep_contract_scan(popularity_meta):
            continue
        txs = get_normal_transactions(
            client,
            ex_addr,
            network,
            max_pages_override=CONTRACT_POPULARITY_EARLY_PAGES + 2,
            max_total_rows=HUGE_ADDRESS_TX_THRESHOLD,
        )
        if SKIP_DEEP_SCAN_FOR_HUGE_ADDRESS and len(txs) >= HUGE_ADDRESS_TX_THRESHOLD:
            continue

        added = 0
        for tx in txs:
            from_addr = normalize_address(tx.get("from"))
            to_addr = normalize_address(tx.get("to"))
            if to_addr != ex_addr or not from_addr or from_addr == target:
                continue
            label = ex_meta.get("label", "Contract")
            candidates.setdefault(from_addr, []).append((ex_addr, label))
            added += 1
            if added >= MAX_WALLETS_PER_EXCHANGE_SCAN:
                break

    batch_populate_contract_cache(client, network, list(candidates.keys()), contract_cache)

    clustered: Dict[str, Dict[str, Any]] = {}
    for wallet, exchanges in candidates.items():
        if contract_cache.get((network, wallet), False):
            continue
        bucket = clustered.setdefault(wallet, {"address": wallet, "shared_exchange_addresses": set(), "shared_exchange_labels": set()})
        for ex_addr, label in exchanges:
            bucket["shared_exchange_addresses"].add(ex_addr)
            bucket["shared_exchange_labels"].add(label)

    for data in clustered.values():
        data["shared_exchange_addresses"] = sorted(data["shared_exchange_addresses"])
        data["shared_exchange_labels"] = sorted(data["shared_exchange_labels"])
    return clustered


def analyze_shared_exchanges(
    client: httpx.Client,
    network: str,
    wallets: List[str],
    contract_cache: Dict[Tuple[str, str], bool],
) -> Dict[str, Dict[str, Any]]:
    unique_wallets = list(dict.fromkeys([w for w in wallets if w]))[:MAX_CLUSTER_WALLETS_FOR_EXCHANGE_CHECK]

    wallet_destinations: Dict[str, Set[str]] = {}
    all_destinations: Set[str] = set()

    for wallet in unique_wallets:
        txs = get_normal_transactions(
            client,
            wallet,
            network,
            max_pages_override=SHARED_SCAN_WALLET_MAX_PAGES,
            max_total_rows=SHARED_SCAN_WALLET_MAX_TXS,
        )
        dests: Set[str] = set()
        for tx in txs:
            from_addr = normalize_address(tx.get("from"))
            to_addr = normalize_address(tx.get("to"))
            if from_addr != wallet or not to_addr or to_addr == wallet:
                continue
            if to_addr == ZERO_ADDRESS:
                continue
            try:
                value = int(tx.get("value", "0") or "0")
            except (ValueError, TypeError):
                value = 0
            if value == 0:
                continue
            dests.add(to_addr)
            all_destinations.add(to_addr)
        wallet_destinations[wallet] = dests

    uncached = [a for a in all_destinations if (network, a) not in contract_cache]
    if uncached:
        batch_populate_contract_cache(client, network, uncached, contract_cache)

    exchange_map: Dict[str, Dict[str, Any]] = {}
    for wallet, dests in wallet_destinations.items():
        for dest in dests:
            if not contract_cache.get((network, dest), False):
                continue
            bucket = exchange_map.setdefault(dest, {"exchange_address": dest, "label": "Contract", "source_wallets": set()})
            bucket["source_wallets"].add(wallet)

    result: Dict[str, Dict[str, Any]] = {}
    for ex_addr, data in exchange_map.items():
        if len(data["source_wallets"]) >= 2:
            data["source_wallets"] = sorted(data["source_wallets"])
            result[ex_addr] = data
    return result


def fetch_txs_for_wallets(client: httpx.Client, network: str, wallets: List[str]) -> Dict[str, List[Dict[str, Any]]]:
    result: Dict[str, List[Dict[str, Any]]] = {}

    def fetch_one(wallet: str) -> Tuple[str, List[Dict[str, Any]]]:
        txs = get_normal_transactions(
            client,
            wallet,
            network,
            max_pages_override=CANDIDATE_WALLET_MAX_PAGES,
            max_total_rows=CANDIDATE_WALLET_MAX_TXS,
        )
        return wallet, txs

    unique_wallets = list(dict.fromkeys(wallets))[:MAX_PROMISING_CANDIDATES_PER_NETWORK]
    if not unique_wallets:
        return result

    worker_count = min(8, len(unique_wallets))
    with ThreadPoolExecutor(max_workers=max(1, worker_count)) as pool:
        futures = [pool.submit(fetch_one, wallet) for wallet in unique_wallets]
        for future in as_completed(futures):
            wallet, txs = future.result()
            result[wallet] = txs
    return result


def find_shared_funding_sources(
    client: httpx.Client,
    target: str,
    candidate_wallets: List[str],
    network_txs_by_wallet: Dict[str, List[Dict[str, Any]]],
    contract_cache: Dict[Tuple[str, str], bool],
    network: str,
) -> Dict[str, List[str]]:
    def incoming_sources(wallet: str, txs: List[Dict[str, Any]]) -> Set[str]:
        sources: Set[str] = set()
        for tx in txs:
            from_addr = normalize_address(tx.get("from"))
            to_addr = normalize_address(tx.get("to"))
            if to_addr != wallet or not from_addr or from_addr == wallet:
                continue
            if from_addr == ZERO_ADDRESS:
                continue
            if contract_cache.get((network, from_addr), False):
                continue
            sources.add(from_addr)
        return sources

    all_sources: Set[str] = set()
    for wallet, txs in network_txs_by_wallet.items():
        for tx in txs:
            from_addr = normalize_address(tx.get("from"))
            to_addr = normalize_address(tx.get("to"))
            if to_addr == wallet and from_addr and from_addr != ZERO_ADDRESS and from_addr != wallet:
                all_sources.add(from_addr)
    if all_sources:
        batch_populate_contract_cache(client, network, list(all_sources), contract_cache)  # type: ignore[name-defined]

    target_sources = incoming_sources(target, network_txs_by_wallet.get(target, []))
    result: Dict[str, List[str]] = {}
    for wallet in candidate_wallets:
        wallet_sources = incoming_sources(wallet, network_txs_by_wallet.get(wallet, []))
        shared = sorted(target_sources & wallet_sources)
        if shared:
            result[wallet] = shared
    return result


def compute_first_funders(
    client: httpx.Client,
    network: str,
    txs_by_wallet: Dict[str, List[Dict[str, Any]]],
    contract_cache: Dict[Tuple[str, str], bool],
) -> Dict[str, Dict[str, Any]]:
    all_sources: Set[str] = set()
    for wallet, txs in txs_by_wallet.items():
        for tx in txs:
            from_addr = normalize_address(tx.get("from"))
            to_addr = normalize_address(tx.get("to"))
            if to_addr == wallet and from_addr and from_addr not in (wallet, ZERO_ADDRESS):
                all_sources.add(from_addr)
    if all_sources:
        batch_populate_contract_cache(client, network, list(all_sources), contract_cache)

    result: Dict[str, Dict[str, Any]] = {}
    for wallet, txs in txs_by_wallet.items():
        ordered = sorted(txs, key=lambda x: int(x.get("timeStamp", "0") or "0"))
        for tx in ordered:
            from_addr = normalize_address(tx.get("from"))
            to_addr = normalize_address(tx.get("to"))
            if to_addr != wallet or not from_addr or from_addr in (wallet, ZERO_ADDRESS):
                continue
            if contract_cache.get((network, from_addr), False):
                continue
            try:
                value = int(tx.get("value", "0") or "0")
            except Exception:
                value = 0
            if value <= 0:
                continue
            result[wallet] = {
                "funder": from_addr,
                "value": str(value),
                "tx_hash": tx.get("hash"),
                "timeStamp": tx.get("timeStamp", "0"),
            }
            break
    return result


def apply_post_fetch_signals(
    target: str,
    network: str,
    candidates: Dict[str, Dict[str, Any]],
    promising_wallets: List[str],
    network_txs_by_wallet: Dict[str, List[Dict[str, Any]]],
    shared_funding_sources: Dict[str, List[str]],
    first_funders: Dict[str, Dict[str, Any]],
) -> None:
    target_first_funder = first_funders.get(target)

    for wallet in promising_wallets:
        c = candidates.get(wallet)
        if not c:
            continue

        sources = shared_funding_sources.get(wallet, [])
        if sources:
            c["shared_funding_sources"] = list(sources)
            add_signal(c, "shared_funding_source", WEIGHT_SHARED_FUNDING_SOURCE, f"sources={len(sources)}")

        wallet_first_funder = first_funders.get(wallet)
        if target_first_funder and wallet_first_funder and wallet_first_funder["funder"] == target_first_funder["funder"]:
            c["shared_first_funder"] = wallet_first_funder["funder"]
            add_signal(c, "shared_first_funder", WEIGHT_FIRST_FUNDER_SHARED, wallet_first_funder["funder"])

        target_txs = network_txs_by_wallet.get(target, [])
        cand_txs = network_txs_by_wallet.get(wallet, [])
        if not cand_txs:
            finalize_candidate_confidence(c)
            continue

        prox = estimate_time_proximity(target, wallet, target_txs, cand_txs)
        c["time_proximity_hits"] = prox["hits_1h"] + prox["hits_24h"]
        if prox["hits_1h"] > 0:
            add_signal(c, "time_proximity_1h", WEIGHT_TIME_PROXIMITY_1H, f"hits={prox['hits_1h']}")
        elif prox["hits_24h"] > 0:
            add_signal(c, "time_proximity_24h", WEIGHT_TIME_PROXIMITY_24H, f"hits={prox['hits_24h']}")

        amount_hits = estimate_amount_similarity(target, wallet, target_txs, cand_txs)
        c["similar_amount_hits"] = amount_hits
        if amount_hits > 0:
            add_signal(c, "similar_amount", WEIGHT_SIMILAR_AMOUNT, f"hits={amount_hits}")

        finalize_candidate_confidence(c)

    for wallet, c in candidates.items():
        if wallet not in promising_wallets:
            finalize_candidate_confidence(c)


# =========================================================
# ETHOS
# =========================================================
def check_ethos_account(client: httpx.Client, address: str) -> Dict[str, Any]:
    url = f"{ETHOS_SINGLE_USER_BY_ADDRESS_URL}/{address}"
    result = http_request_with_retries(
        client,
        "GET",
        url,
        op_name=f"ethos:by_address:{address}",
        max_retries=MAX_RETRIES,
        retry_on_statuses=ETHOS_RETRYABLE_STATUSES,
    )

    if not result["ok"]:
        return {"address": address, "exists": None, "status": None, "username": None, "profile_url": None, "error": result.get("error") or "request_failed"}

    resp = result["response"]
    if resp is None:
        return {"address": address, "exists": None, "status": None, "username": None, "profile_url": None, "error": "empty_response"}

    if resp.status_code == 404:
        return {"address": address, "exists": False, "status": None, "username": None, "profile_url": None, "error": None}

    if resp.status_code != 200:
        return {"address": address, "exists": None, "status": None, "username": None, "profile_url": None, "error": f"http_{resp.status_code}"}

    try:
        data = resp.json()
    except Exception as e:
        return {"address": address, "exists": None, "status": None, "username": None, "profile_url": None, "error": f"invalid_json: {e}"}

    links = data.get("links") or {}
    username = data.get("username")
    profile_url = links.get("profile")
    if not profile_url and username:
        profile_url = f"https://app.ethos.network/profile/x/{username}"

    return {
        "address": address,
        "exists": True,
        "status": data.get("status"),
        "username": username,
        "profile_url": profile_url,
        "error": None,
    }


def check_ethos_accounts_bulk(client: httpx.Client, addresses: List[str]) -> List[Dict[str, Any]]:
    unique_addresses = list(dict.fromkeys([a for a in addresses if a]))
    if not unique_addresses:
        return []

    results_by_address: Dict[str, Dict[str, Any]] = {}
    for i in range(0, len(unique_addresses), ETHOS_BULK_MAX_ADDRESSES):
        chunk = unique_addresses[i:i + ETHOS_BULK_MAX_ADDRESSES]

        result = http_request_with_retries(
            client,
            "POST",
            ETHOS_USERS_BY_ADDRESSES_URL,
            op_name=f"ethos:bulk_by_address[{len(chunk)}]",
            max_retries=MAX_RETRIES,
            retry_on_statuses=ETHOS_RETRYABLE_STATUSES,
            json={"addresses": chunk},
        )

        if not result["ok"]:
            err = result.get("error") or "bulk_request_failed"
            for addr in chunk:
                results_by_address[addr] = {"address": addr, "exists": None, "status": None, "username": None, "profile_url": None, "error": err}
            continue

        resp = result["response"]
        if resp is None:
            for addr in chunk:
                results_by_address[addr] = {"address": addr, "exists": None, "status": None, "username": None, "profile_url": None, "error": "empty_response"}
            continue

        if resp.status_code != 200:
            for addr in chunk:
                results_by_address[addr] = {"address": addr, "exists": None, "status": None, "username": None, "profile_url": None, "error": f"http_{resp.status_code}"}
            continue

        try:
            data = resp.json()
        except Exception as e:
            for addr in chunk:
                results_by_address[addr] = {"address": addr, "exists": None, "status": None, "username": None, "profile_url": None, "error": f"invalid_json: {e}"}
            continue

        if not isinstance(data, list):
            data = []

        returned_addresses: Set[str] = set()
        for item in data:
            if not isinstance(item, dict):
                continue
            wallet_address = normalize_address(item.get("walletAddress") or item.get("address"))
            username = item.get("username")
            links = item.get("links") or {}
            profile_url = links.get("profile")
            if not profile_url and username:
                profile_url = f"https://app.ethos.network/profile/x/{username}"
            if wallet_address:
                returned_addresses.add(wallet_address)
                results_by_address[wallet_address] = {
                    "address": wallet_address,
                    "exists": True,
                    "status": item.get("status"),
                    "username": username,
                    "profile_url": profile_url,
                    "error": None,
                }

        for addr in chunk:
            if addr not in returned_addresses and addr not in results_by_address:
                results_by_address[addr] = check_ethos_account(client, addr)

    return [results_by_address[addr] for addr in unique_addresses]


def is_ethos_active(item: Dict[str, Any]) -> bool:
    if item.get("exists") is not True:
        return False
    if not item.get("profile_url"):
        return False
    status = item.get("status")
    if status is None:
        return False
    return str(status).strip().lower() == "active"


def build_final_ethos_summary(target: str, main_ethos: Dict[str, Any], linked_ethos_results: List[Dict[str, Any]], lang: str = "ru") -> str:
    active_linked = [x for x in linked_ethos_results if is_ethos_active(x)]
    lines: List[str] = ["=== ETHOS SUMMARY ===", "", f"🟢 Main address: {target}"]

    if main_ethos.get("error"):
        lines.append(f"   Ethos: ошибка проверки ({main_ethos['error']})")
    elif is_ethos_active(main_ethos):
        lines.append(
            f"   Ethos: {main_ethos['profile_url']} | user={main_ethos.get('username') or 'n/a'} | status={main_ethos.get('status') or 'n/a'}"
        )
    else:
        lines.append("   Ethos: активный аккаунт не найден")

    lines.append("")
    if active_linked:
        lines.append("🔴 Сильные связанные адреса с активным Ethos:")
        for item in active_linked:
            lines.append(
                f"- {item['address']} | user={item.get('username') or 'n/a'} | status={item.get('status') or 'n/a'} | {item['profile_url']}"
            )
    else:
        errored = [x for x in linked_ethos_results if x.get("error")]
        if errored:
            lines.append("🔴 Проверка части связанных адресов завершилась с ошибками:")
            for item in errored[:10]:
                lines.append(f"- {item['address']} | error={item.get('error')}")
        else:
            lines.append("🔴 Сильных связанных адресов с активным Ethos не найдено.")

    return "\n".join(lines)


# =========================================================
# REPORTS
# =========================================================
def build_network_report(
    target: str,
    network: str,
    tx_count: int,
    direct_wallets: Dict[str, Dict[str, Any]],
    candidates: Dict[str, Dict[str, Any]],
    final_cluster_wallets: Dict[str, Dict[str, Any]],
    possible_cluster_wallets: Dict[str, Dict[str, Any]],
    shared_funding_sources: Dict[str, List[str]],
    target_first_funder: Optional[Dict[str, Any]],
    shared_first_funder_wallets: List[str],
    shared_exchanges: Optional[Dict[str, Dict[str, Any]]] = None,
) -> str:
    lines: List[str] = [
        f"=== {network.upper()} ===",
        f"Target: {target}",
        f"Normal tx fetched: {tx_count}",
        "",
        f"1) Direct EOA wallets: {len(direct_wallets)}",
    ]

    for addr, info in sorted(direct_wallets.items(), key=lambda x: (-x[1]["count"], x[0]))[:REPORT_MAX_DIRECT]:
        bi = " | bidirectional=yes" if info.get("bidirectional") else ""
        rep = " | repeat=yes" if info.get("repeat_transfer") else ""
        lines.append(f"- {addr} | total={info['count']} | in={info['incoming_count']} | out={info['outgoing_count']}{bi}{rep}")

    lines.extend([
        "",
        f"2) Pre-scored candidates: {len(candidates)}",
        f"3) Shared funding sources: {len(shared_funding_sources)} wallets",
    ])

    if target_first_funder:
        lines.append(
            f"4) Target first funder: {target_first_funder['funder']} | value={target_first_funder['value']} | time={ts_to_human(target_first_funder['timeStamp'])}"
        )
    else:
        lines.append("4) Target first funder: not found")

    if shared_first_funder_wallets:
        lines.append(f"5) Shared first funder wallets: {len(shared_first_funder_wallets)}")
        for wallet in shared_first_funder_wallets[:REPORT_MAX_DIRECT]:
            funder = candidates.get(wallet, {}).get("shared_first_funder")
            lines.append(f"- {wallet} | funder={funder}")
    else:
        lines.append("5) Shared first funder wallets: 0")

    lines.append("")
    lines.append("6) Strong cluster:")
    if not final_cluster_wallets:
        lines.append("- none")
    else:
        for addr, info in sorted(final_cluster_wallets.items(), key=lambda x: (-x[1]["score"], x[0]))[:REPORT_MAX_CLUSTER]:
            signal_names = ", ".join(sorted(info.get("signal_types", [])))
            lines.append(f"- {addr} | score={info['score']} | signals={signal_names}")

    lines.append("")
    lines.append("7) Possible candidates:")
    if not possible_cluster_wallets:
        lines.append("- none")
    else:
        for addr, info in sorted(possible_cluster_wallets.items(), key=lambda x: (-x[1]["score"], x[0]))[:REPORT_MAX_CLUSTER]:
            signal_names = ", ".join(sorted(info.get("signal_types", [])))
            lines.append(f"- {addr} | score={info['score']} | signals={signal_names}")

    if shared_exchanges:
        lines.append("")
        lines.append(f"8) Shared contract destinations: {len(shared_exchanges)}")
        for ex_addr, data in sorted(shared_exchanges.items(), key=lambda x: (-len(x[1]['source_wallets']), x[0]))[:REPORT_MAX_SHARED]:
            wallets = list(data["source_wallets"])
            lines.append(f"- {ex_addr} | wallets={len(wallets)}")
            for w in wallets[:REPORT_MAX_WALLETS_PER_EXCHANGE]:
                lines.append(f"    • {w}")

    return "\n".join(lines)


def build_summary(all_reports: Dict[str, Dict[str, Any]]) -> str:
    lines = ["=== FINAL SUMMARY ==="]
    total_strong = 0
    total_possible = 0
    for network, data in all_reports.items():
        final_cluster_wallets = data.get("final_cluster_wallets", {})
        possible_cluster_wallets = data.get("possible_cluster_wallets", {})
        total_strong += len(final_cluster_wallets)
        total_possible += len(possible_cluster_wallets)
        lines.append(
            f"- {network}: tx={data['tx_count']}, direct={len(data['direct_wallets'])}, promising={len(data.get('promising_wallets', []))}, strong={len(final_cluster_wallets)}, possible={len(possible_cluster_wallets)}"
        )
    lines.append(f"Total: {total_strong} strong, {total_possible} possible candidates")
    return "\n".join(lines)


def build_address_map_message(target: str, all_reports: Dict[str, Dict[str, Any]]) -> str:
    strong_addresses: Set[str] = set()
    possible_addresses: Set[str] = set()
    for network_data in all_reports.values():
        for addr in network_data.get("final_cluster_wallets", {}):
            if addr != target:
                strong_addresses.add(addr)
        for addr in network_data.get("possible_cluster_wallets", {}):
            if addr != target and addr not in strong_addresses:
                possible_addresses.add(addr)

    lines: List[str] = [
        "=== LINKED ADDRESSES MAP ===",
        "",
        f"🟢 {target} — main address",
        "",
        "🔴 Strong cluster:",
    ]
    if strong_addresses:
        for addr in sorted(strong_addresses):
            lines.append(f"🔴 {addr}")
    else:
        lines.append("🔴 No strong cluster found.")

    lines.append("")
    lines.append("🟡 Possible candidates:")
    if possible_addresses:
        for addr in sorted(possible_addresses):
            lines.append(f"🟡 {addr}")
    else:
        lines.append("🟡 No possible candidates found.")

    return "\n".join(lines)


def collect_related_addresses(target: str, all_reports: Dict[str, Dict[str, Any]]) -> List[str]:
    related: Set[str] = set()
    for network_data in all_reports.values():
        for addr in network_data.get("final_cluster_wallets", {}):
            if addr != target:
                related.add(addr)
    return sorted(related)


# =========================================================
# NETWORK WORKER
# =========================================================
def analyze_single_network(client: httpx.Client, target: str, network: str, chat_id: str) -> Dict[str, Any]:
    _check_cancelled(chat_id)
    contract_cache: Dict[Tuple[str, str], bool] = {}

    display_name = network.capitalize()
    log(f"[{display_name}] Fetching target transactions...")
    txs = get_normal_transactions(client, target, network)
    log(f"[{display_name}] {len(txs)} transactions fetched")
    _check_cancelled(chat_id)

    log(f"[{display_name}] Analyzing direct transfers...")
    direct_wallets = analyze_direct_wallet_transfers(client, target, network, txs, contract_cache)
    log(f"[{display_name}] {len(direct_wallets)} direct EOA wallets found")
    _check_cancelled(chat_id)

    exchange_cluster_wallets: Dict[str, Dict[str, Any]] = {}
    contract_popularity_map: Dict[str, Dict[str, Any]] = {}
    if ENABLE_EXCHANGE_CLUSTER_SCAN:
        target_exchange_addresses = get_exchange_addresses_from_txs(target, network, txs, contract_cache)
        contract_popularity_map = build_contract_popularity_map(client, network, target_exchange_addresses)
        exchange_cluster_wallets = find_wallets_sharing_exchanges(
            client=client,
            target=target,
            network=network,
            exchange_addresses=target_exchange_addresses,
            contract_cache=contract_cache,
            contract_popularity_map=contract_popularity_map,
        )
        exchange_cluster_wallets = {addr: info for addr, info in exchange_cluster_wallets.items() if addr not in direct_wallets}

    candidates = build_pre_candidate_scores(direct_wallets, exchange_cluster_wallets, contract_popularity_map)
    promising_wallets = [
        addr for addr, info in sorted(candidates.items(), key=lambda x: (-x[1]["pre_score"], x[0]))
        if info["pre_score"] >= PRE_SCORE_THRESHOLD
    ][:MAX_PROMISING_CANDIDATES_PER_NETWORK]
    log(f"[{display_name}] Pre-scored {len(candidates)} candidates, {len(promising_wallets)} promising (pre-score >= {PRE_SCORE_THRESHOLD})")
    _check_cancelled(chat_id)

    network_txs_by_wallet: Dict[str, List[Dict[str, Any]]] = {target: txs}
    if promising_wallets:
        log(f"[{display_name}] Fetching transactions for {len(promising_wallets)} promising candidates...")
        network_txs_by_wallet.update(fetch_txs_for_wallets(client, network, promising_wallets))
    _check_cancelled(chat_id)

    log(f"[{display_name}] Checking shared funding sources...")
    shared_funding_sources = find_shared_funding_sources(client, target, promising_wallets, network_txs_by_wallet, contract_cache, network)
    log(f"[{display_name}] {len(shared_funding_sources)} wallets share funding sources")
    _check_cancelled(chat_id)

    wallets_for_first_funder = {target: network_txs_by_wallet.get(target, [])}
    for wallet in promising_wallets:
        wallets_for_first_funder[wallet] = network_txs_by_wallet.get(wallet, [])

    log(f"[{display_name}] Checking first funders for target + {len(promising_wallets)} candidates...")
    first_funders = compute_first_funders(client, network, wallets_for_first_funder, contract_cache)
    target_first_funder = first_funders.get(target)
    if target_first_funder:
        log(f"[{display_name}] Target first funder: {target_first_funder['funder'][:10]}... ({target_first_funder['value']})")
    else:
        log(f"[{display_name}] Target first funder: not found")

    shared_first_funder_wallets: List[str] = []
    for wallet in promising_wallets:
        wf = first_funders.get(wallet)
        if target_first_funder and wf and wf["funder"] == target_first_funder["funder"]:
            shared_first_funder_wallets.append(wallet)
            log(f"[{display_name}] Shared first funder! {wallet[:10]}... and target both funded by {wf['funder'][:10]}...")
    log(f"[{display_name}] {len(shared_first_funder_wallets)} candidates share first funder with target")
    _check_cancelled(chat_id)

    log(f"[{display_name}] Scoring candidates...")
    apply_post_fetch_signals(
        target=target,
        network=network,
        candidates=candidates,
        promising_wallets=promising_wallets,
        network_txs_by_wallet=network_txs_by_wallet,
        shared_funding_sources=shared_funding_sources,
        first_funders=first_funders,
    )
    final_cluster_wallets, possible_cluster_wallets = split_candidates_by_confidence(candidates)
    log(f"[{display_name}] Scoring complete: {len(final_cluster_wallets)} strong, {len(possible_cluster_wallets)} possible")
    _check_cancelled(chat_id)

    shared_exchanges: Dict[str, Dict[str, Any]] = {}
    if ENABLE_SHARED_EXCHANGE_SCAN and final_cluster_wallets:
        cluster_wallets = [target] + sorted(final_cluster_wallets.keys())
        log(f"[{display_name}] Checking for shared CEX deposit addresses...")
        shared_exchanges = analyze_shared_exchanges(client, network, cluster_wallets, contract_cache)

    return {
        "tx_count": len(txs),
        "direct_wallets": direct_wallets,
        "exchange_cluster_wallets": exchange_cluster_wallets,
        "candidate_scores": candidates,
        "final_cluster_wallets": final_cluster_wallets,
        "possible_cluster_wallets": possible_cluster_wallets,
        "shared_funding_sources": shared_funding_sources,
        "first_funders": first_funders,
        "target_first_funder": target_first_funder,
        "shared_first_funder_wallets": shared_first_funder_wallets,
        "promising_wallets": promising_wallets,
        "shared_exchanges": shared_exchanges,
    }


# =========================================================
# MAIN WALLET ANALYSIS
# =========================================================
def analyze_wallet(client: httpx.Client, target: str, chat_id: str) -> None:
    lang = get_lang(chat_id)
    start_time = time.monotonic()

    networks = list(DEFAULT_NETWORKS)
    if ENABLE_BSC:
        networks.append("bsc")

    send_telegram_message(
        client,
        chat_id,
        msg("start_analysis", lang, target=target, networks=", ".join(networks)),
    )
    log(f"Starting cluster scan for {target}")
    log(f"Networks: {', '.join([n.capitalize() for n in networks])}")

    all_reports: Dict[str, Dict[str, Any]] = {}
    future_to_network: Dict[Any, str] = {}

    with ThreadPoolExecutor(max_workers=min(MAX_NETWORK_WORKERS, len(networks))) as pool:
        for network in networks:
            future = pool.submit(analyze_single_network, client, target, network, chat_id)
            future_to_network[future] = network

        for future in as_completed(future_to_network):
            network = future_to_network[future]
            _check_cancelled(chat_id)
            report = future.result()
            all_reports[network] = report
            send_telegram_message(
                client,
                chat_id,
                build_network_report(
                    target=target,
                    network=network,
                    tx_count=report["tx_count"],
                    direct_wallets=report["direct_wallets"],
                    candidates=report["candidate_scores"],
                    final_cluster_wallets=report["final_cluster_wallets"],
                    possible_cluster_wallets=report["possible_cluster_wallets"],
                    shared_funding_sources=report["shared_funding_sources"],
                    target_first_funder=report["target_first_funder"],
                    shared_first_funder_wallets=report["shared_first_funder_wallets"],
                    shared_exchanges=report["shared_exchanges"],
                ),
            )

    send_telegram_message(client, chat_id, build_summary(all_reports))
    send_telegram_message(client, chat_id, build_address_map_message(target, all_reports))

    related_addresses = collect_related_addresses(target, all_reports)
    send_telegram_message(client, chat_id, msg("ethos_stage", lang, n=len(related_addresses)))

    log(f"Checking {1 + len(related_addresses)} addresses on Ethos Network (bulk)...")
    ethos_all = check_ethos_accounts_bulk(client, [target] + related_addresses)
    ethos_by_addr = {item["address"]: item for item in ethos_all}

    main_ethos = ethos_by_addr.get(target) or {
        "address": target,
        "exists": None,
        "status": None,
        "username": None,
        "profile_url": None,
        "error": "missing_bulk_result",
    }
    linked_ethos_results = [ethos_by_addr[a] for a in related_addresses if a in ethos_by_addr]

    send_telegram_message(
        client,
        chat_id,
        build_final_ethos_summary(
            target=target,
            main_ethos=main_ethos,
            linked_ethos_results=linked_ethos_results,
            lang=lang,
        ),
    )

    elapsed = time.monotonic() - start_time
    send_telegram_message(client, chat_id, f"Cluster scan complete in {elapsed / 60:.1f}m")
    log(f"Cluster scan complete in {elapsed:.1f}s")


# =========================================================
# TELEGRAM ROUTER
# =========================================================
def _run_analysis_thread(client: httpx.Client, wallet: str, chat_id: str) -> None:
    lang = get_lang(chat_id)
    try:
        analyze_wallet(client, wallet, chat_id)
    except AnalysisCancelledError:
        log(f"[cancel] analysis cancelled for chat_id={chat_id}")
        send_telegram_message(client, chat_id, msg("cancel_done", lang))
    except Exception as e:
        log(f"analyze_wallet failed: {exception_name(e)}: {e}")
        send_telegram_message(client, chat_id, msg("analyze_error", lang, error=f"{exception_name(e)}: {e}"))
    finally:
        with _threads_lock:
            _active_threads.pop(chat_id, None)
            _cancel_events.pop(chat_id, None)


def handle_message(client: httpx.Client, message: Dict[str, Any]) -> None:
    chat = message.get("chat", {})
    chat_id = str(chat.get("id"))
    text = (message.get("text") or "").strip()

    if not chat_id:
        return

    if TELEGRAM_ALLOWED_CHAT_ID and str(TELEGRAM_ALLOWED_CHAT_ID) != chat_id:
        log(f"Ignored message from unauthorized chat_id={chat_id}")
        return

    lang = get_lang(chat_id)

    if text == "/cancel":
        with _threads_lock:
            thread = _active_threads.get(chat_id)
            event = _cancel_events.get(chat_id)
        if thread and thread.is_alive() and event:
            event.set()
            send_telegram_message(client, chat_id, msg("cancel_requested", lang))
        else:
            send_telegram_message(client, chat_id, msg("cancel_no_task", lang))
        return

    if text.startswith("/lang"):
        parts = text.split()
        if len(parts) >= 2:
            new_lang = parts[1].lower()
            if new_lang in ("ru", "en"):
                set_lang(chat_id, new_lang)
                send_telegram_message(client, chat_id, msg(f"lang_set_{new_lang}", new_lang))
            else:
                send_telegram_message(client, chat_id, msg("lang_unknown", lang))
        else:
            send_telegram_message(client, chat_id, msg("lang_unknown", lang))
        return

    if text in ("/start", "/help"):
        send_telegram_message(client, chat_id, msg("help", lang))
        return

    wallet = normalize_address(text)
    if not wallet:
        send_telegram_message(client, chat_id, msg("invalid_address", lang))
        return

    with _threads_lock:
        existing = _active_threads.get(chat_id)
        if existing and existing.is_alive():
            send_telegram_message(client, chat_id, msg("already_running", lang))
            return

        event = threading.Event()
        _cancel_events[chat_id] = event
        thread = threading.Thread(
            target=_run_analysis_thread,
            args=(client, wallet, chat_id),
            daemon=True,
            name=f"analysis-{chat_id}",
        )
        _active_threads[chat_id] = thread

    thread.start()


# =========================================================
# MAIN LOOP
# =========================================================
def validate_env() -> None:
    missing = []
    if not ALCHEMY_API_KEY or ALCHEMY_API_KEY.startswith("PASTE_"):
        missing.append("ALCHEMY_API_KEY")
    if not ANKR_API_KEY or ANKR_API_KEY.startswith("PASTE_"):
        missing.append("ANKR_API_KEY")
    if not TELEGRAM_TOKEN or TELEGRAM_TOKEN.startswith("PASTE_"):
        missing.append("TELEGRAM_TOKEN")
    if missing:
        raise RuntimeError(f"Missing required env/config values: {', '.join(missing)}")


def main() -> None:
    validate_env()

    log("Bot started")
    log(f"Providers: Alchemy ({', '.join(ALCHEMY_NETWORK_URLS)}) | ANKR (bsc)")
    if TELEGRAM_ALLOWED_CHAT_ID:
        log(f"Allowed chat id: {TELEGRAM_ALLOWED_CHAT_ID}")
    else:
        log("Allowed chat id: not set (all chats allowed)")

    with build_http_client() as client:
        offset: Optional[int] = None

        me = telegram_get(client, "getMe", {})
        if me.get("ok"):
            log(f"Telegram bot connected: {safe_json(me.get('data'))}")

        while True:
            try:
                updates = get_updates(client, offset)
                for upd in updates:
                    offset = upd["update_id"] + 1
                    message = upd.get("message")
                    if not message:
                        continue
                    handle_message(client, message)
            except KeyboardInterrupt:
                log("Bot stopped by user")
                break
            except Exception as e:
                log(f"Main loop error: {exception_name(e)}: {e}")
                time.sleep(SLEEP_ON_ERROR_SECONDS)


if __name__ == "__main__":
    main()
