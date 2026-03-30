#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import time
import json
import hashlib
import threading
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple

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

ALCHEMY_MAX_PAGES = 50

MAX_CLUSTER_WALLETS_FOR_EXCHANGE_CHECK = 30
MAX_WALLETS_PER_EXCHANGE_SCAN   = 100
MAX_EXCHANGE_CONTRACTS_TO_SCAN  = 10
MAX_CANDIDATES_PER_NETWORK      = 150

REPORT_MAX_DIRECT = 30
REPORT_MAX_CLUSTER = 30
REPORT_MAX_SHARED = 20
REPORT_MAX_WALLETS_PER_EXCHANGE = 5

TELEGRAM_MIN_SEND_INTERVAL = 1.1
TELEGRAM_MAX_RETRIES_429   = 5

CHECKPOINT_DIR = os.path.join(os.path.expanduser("~"), ".eths_checkpoints")

DEFAULT_NETWORKS = ["ethereum", "arbitrum", "base", "optimism", "polygon", "bsc"]

# Early stop for huge contract addresses
CONTRACT_POPULARITY_EARLY_PAGES = 3
EARLY_POPULAR_SENDER_THRESHOLD = 120
EARLY_VERY_POPULAR_SENDER_THRESHOLD = 600

# Hard guard against huge transaction histories
HUGE_ADDRESS_TX_THRESHOLD = 15000
SKIP_DEEP_SCAN_FOR_HUGE_ADDRESS = True

# Candidate-wallet fetch caps
CANDIDATE_WALLET_MAX_PAGES = 6
CANDIDATE_WALLET_MAX_TXS = 4000

# Shared-scan wallet caps
SHARED_SCAN_WALLET_MAX_PAGES = 8
SHARED_SCAN_WALLET_MAX_TXS = 6000


# =========================================================
# SCORING / HEURISTICS
# =========================================================
FINAL_SCORE_THRESHOLD = 8
POSSIBLE_SCORE_THRESHOLD = 4

WEIGHT_DIRECT_ONCE = 4
WEIGHT_DIRECT_REPEAT = 4
WEIGHT_BIDIRECTIONAL = 3
WEIGHT_SHARED_RARE_CONTRACT = 3
WEIGHT_SHARED_MEDIUM_CONTRACT = 1
WEIGHT_SHARED_POPULAR_CONTRACT = -2
WEIGHT_SHARED_FUNDING_SOURCE = 5
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
            "1) найду прямые связи между кошельками\n"
            "2) найду адреса через общий контракт/депозит\n"
            "3) посчитаю score подозрительности\n"
            "4) проверю сильных кандидатов в Ethos Network\n"
            "5) для EVM-сетей использую Alchemy, для BSC — ANKR\n"
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
            "2) find addresses via shared contract/deposit\n"
            "3) score suspicious candidates\n"
            "4) check strong candidates in Ethos Network\n"
            "5) use Alchemy for EVM networks, ANKR for BSC\n"
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
        "ru": "Старт анализа: {target}\nСети: {networks}",
        "en": "Starting analysis: {target}\nNetworks: {networks}",
    },
    "scanning_network": {
        "ru": "[{network}] Сканирую...",
        "en": "[{network}] Scanning...",
    },
    "ethos_stage": {
        "ru": "Этап Ethos: проверяю главный адрес и {n} сильных адресов кластера...",
        "en": "Ethos stage: checking main address and {n} strong cluster addresses...",
    },
    "no_direct_transfers": {
        "ru": "1) Прямых переводов target <-> EOA не найдено.",
        "en": "1) No direct target <-> EOA transfers found.",
    },
    "direct_transfers_found": {
        "ru": "1) Найдены прямые переводы с другими кошельками (EOA):",
        "en": "1) Direct transfers with other wallets (EOA) found:",
    },
    "no_exchange_cluster": {
        "ru": "2) Дополнительных адресов через общий контракт/депозит не найдено.",
        "en": "2) No additional addresses via shared contract/deposit found.",
    },
    "exchange_cluster_found": {
        "ru": "2) Найдены адреса через общий контракт/депозит:",
        "en": "2) Addresses clustered via shared contract/deposit:",
    },
    "no_repeated_transfers": {
        "ru": "3) Повторных переводов (>1) не найдено.",
        "en": "3) No repeated transfers (>1) found.",
    },
    "repeated_transfers_found": {
        "ru": "3) Повторные переводы (>1) найдены с адресами:",
        "en": "3) Repeated transfers (>1) found with:",
    },
    "no_shared_exchanges": {
        "ru": "4) Общих контрактных адресов для 2+ кошельков не найдено.",
        "en": "4) No shared contract addresses for 2+ wallets found.",
    },
    "shared_exchanges_found": {
        "ru": "4) Найдены общие контрактные адреса для 2+ кошельков:",
        "en": "4) Shared contract addresses for 2+ wallets found:",
    },
    "ethos_summary_title": {
        "ru": "=== ИТОГ ETHOS ===",
        "en": "=== ETHOS SUMMARY ===",
    },
    "main_address_line": {
        "ru": "🟢 Главный адрес: {target}",
        "en": "🟢 Main address: {target}",
    },
    "ethos_not_found": {
        "ru": "   Ethos: активный аккаунт не найден",
        "en": "   Ethos: no active account found",
    },
    "linked_ethos_found": {
        "ru": "🔴 Сильные связанные адреса с активным Ethos:",
        "en": "🔴 Strong linked addresses with active Ethos:",
    },
    "no_linked_ethos": {
        "ru": "🔴 Сильных связанных адресов с активным Ethos не найдено.",
        "en": "🔴 No strong linked addresses with active Ethos found.",
    },
    "address_map_title": {
        "ru": "=== КАРТА СВЯЗАННЫХ АДРЕСОВ ===",
        "en": "=== LINKED ADDRESSES MAP ===",
    },
    "main_address_map_line": {
        "ru": "🟢 {target} — главный адрес",
        "en": "🟢 {target} — main address",
    },
    "cluster_addresses": {
        "ru": "🔴 Сильный кластер:",
        "en": "🔴 Strong cluster:",
    },
    "possible_cluster_addresses": {
        "ru": "🟡 Возможные кандидаты:",
        "en": "🟡 Possible candidates:",
    },
    "no_cluster_addresses": {
        "ru": "🔴 Сильный кластер не найден.",
        "en": "🔴 No strong cluster found.",
    },
    "no_possible_cluster_addresses": {
        "ru": "🟡 Возможные кандидаты не найдены.",
        "en": "🟡 No possible candidates found.",
    },
    "shared_exchange_addresses": {
        "ru": "🟠 Общие контрактные адреса:",
        "en": "🟠 Shared contract addresses:",
    },
    "no_shared_exchange_addresses": {
        "ru": "🟠 Общие контрактные адреса не найдены.",
        "en": "🟠 No shared contract addresses found.",
    },
    "final_summary_title": {
        "ru": "=== ИТОГОВАЯ СВОДКА ===",
        "en": "=== FINAL SUMMARY ===",
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
    limits = httpx.Limits(max_connections=20, max_keepalive_connections=5, keepalive_expiry=10.0)
    headers = {
        "User-Agent": "EthosSybilBot/3.0",
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
# ALCHEMY
# =========================================================
def alchemy_rpc(
    client: httpx.Client,
    network: str,
    method: str,
    params: List[Any],
) -> Dict[str, Any]:
    url = ALCHEMY_NETWORK_URLS.get(network)
    if not url or not ALCHEMY_API_KEY or ALCHEMY_API_KEY.startswith("PASTE_"):
        return {"ok": False, "error": "alchemy_not_configured", "data": None}

    payload = {"id": 1, "jsonrpc": "2.0", "method": method, "params": params}

    result = http_request_with_retries(
        client, "POST", url,
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


def alchemy_get_code(client: httpx.Client, network: str, address: str) -> Optional[str]:
    result = alchemy_rpc(client, network, "eth_getCode", [address, "latest"])
    if not result["ok"]:
        log(f"[alchemy] eth_getCode failed network={network} address={address}: {result.get('error')}")
        return None
    return (result.get("data") or {}).get("result")


def get_normal_transactions_alchemy(
    client: httpx.Client,
    address: str,
    network: str,
    *,
    max_pages_override: Optional[int] = None,
    max_total_rows: Optional[int] = None,
) -> List[Dict[str, Any]]:
    if network not in ALCHEMY_NETWORK_URLS:
        log(f"[alchemy] network not configured: {network}")
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
                log(f"[alchemy] getAssetTransfers failed network={network}: {result.get('error')}")
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

    rows.sort(key=lambda x: int(x.get("timeStamp", "0")), reverse=True)
    log(
        f"[alchemy] network={network} fetched {len(rows)} txs for {address}"
        + (
            f" (capped: pages={max_pages_override}, rows={max_total_rows})"
            if max_pages_override is not None or max_total_rows is not None
            else ""
        )
    )
    return rows


# =========================================================
# ANKR (BSC only)
# =========================================================
def ankr_advanced_post(
    client: httpx.Client,
    method: str,
    params: Dict[str, Any],
) -> Dict[str, Any]:
    if not ANKR_API_KEY or ANKR_API_KEY.startswith("PASTE_"):
        return {"ok": False, "error": "ankr_not_configured", "data": None}

    payload = {"id": 1, "jsonrpc": "2.0", "method": method, "params": params}

    result = http_request_with_retries(
        client, "POST", ANKR_ADVANCED_API_URL,
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


def ankr_bsc_get_code(client: httpx.Client, address: str) -> Optional[str]:
    payload = {"id": 1, "jsonrpc": "2.0", "method": "eth_getCode", "params": [address, "latest"]}
    result = http_request_with_retries(
        client, "POST", ANKR_BSC_RPC_URL,
        op_name=f"ankr:bsc:eth_getCode:{address}",
        json=payload,
        max_retries=MAX_RETRIES,
    )
    if not result["ok"]:
        log(f"[ankr] BSC eth_getCode failed address={address}: {result.get('error')}")
        return None
    try:
        data = result["response"].json()
    except Exception as e:
        log(f"[ankr] BSC eth_getCode invalid json address={address}: {e}")
        return None
    if "error" in data:
        log(f"[ankr] BSC eth_getCode rpc error address={address}: {safe_json(data['error'])}")
        return None
    return data.get("result")


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
            "address":    address,
            "pageSize":   10000,
            "descOrder":  True,
        }
        if page_token:
            params["pageToken"] = page_token

        result = ankr_advanced_post(client, "ankr_getTransactionsByAddress", params)
        if not result["ok"]:
            log(f"[ankr] BSC getTransactionsByAddress failed: {result.get('error')}")
            break

        data         = (result.get("data") or {}).get("result") or {}
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
            to_addr   = normalize_address(tx.get("to"))
            if not from_addr or not to_addr:
                continue

            ts_raw = tx.get("timestamp")
            try:
                ts = str(int(ts_raw)) if ts_raw is not None else "0"
            except Exception:
                ts = "0"

            value_hex = tx.get("value", "0x0")
            try:
                value = (
                    str(int(value_hex, 16))
                    if isinstance(value_hex, str) and value_hex.startswith("0x")
                    else str(value_hex or "0")
                )
            except Exception:
                value = "0"

            block_num = tx.get("blockNumber", "")
            try:
                block_num = (
                    str(int(block_num, 16))
                    if isinstance(block_num, str) and block_num.startswith("0x")
                    else str(block_num or "")
                )
            except Exception:
                block_num = str(block_num or "")

            rows.append({
                "hash":        tx_hash,
                "from":        from_addr,
                "to":          to_addr,
                "timeStamp":   ts,
                "value":       value,
                "blockNumber": block_num,
            })

        page_token = data.get("nextPageToken")
        pages += 1
        if not page_token:
            break

    log(
        f"[ankr] BSC fetched {len(rows)} txs for {address}"
        + (
            f" (capped: pages={max_pages_override}, rows={max_total_rows})"
            if max_pages_override is not None or max_total_rows is not None
            else ""
        )
    )
    return rows


# =========================================================
# TRANSACTION FETCHING
# =========================================================
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
        rpc_url  = ANKR_BSC_RPC_URL
        provider = "ankr:bsc"
    elif network in ALCHEMY_NETWORK_URLS:
        rpc_url  = ALCHEMY_NETWORK_URLS[network]
        provider = f"alchemy:{network}"
    else:
        for addr in to_check:
            cache[(network, addr)] = False
        return

    log(f"[contract_check] batch {len(to_check)} addresses on {network} via {provider}")

    for chunk_start in range(0, len(to_check), batch_size):
        chunk = to_check[chunk_start: chunk_start + batch_size]
        batch_payload = [
            {"id": idx, "jsonrpc": "2.0", "method": "eth_getCode", "params": [addr, "latest"]}
            for idx, addr in enumerate(chunk)
        ]

        result = http_request_with_retries(
            client, "POST", rpc_url,
            op_name=f"{provider}:batch_eth_getCode[{len(chunk)}]",
            json=batch_payload,
            max_retries=MAX_RETRIES,
        )

        if not result["ok"]:
            log(f"[contract_check] batch failed on {network}: {result.get('error')}")
            for addr in chunk:
                cache[(network, addr)] = False
            continue

        try:
            responses = result["response"].json()
        except Exception as e:
            log(f"[contract_check] batch invalid json on {network}: {e}")
            for addr in chunk:
                cache[(network, addr)] = False
            continue

        if not isinstance(responses, list):
            responses = [responses]

        id_to_code: Dict[int, str] = {}
        for resp in responses:
            if isinstance(resp, dict):
                rid  = resp.get("id")
                code = resp.get("result") or "0x"
                if rid is not None:
                    id_to_code[int(rid)] = code

        for idx, addr in enumerate(chunk):
            code = id_to_code.get(idx, "0x")
            cache[(network, addr)] = isinstance(code, str) and code not in ("0x", "0x0")


# =========================================================
# SCORING HELPERS
# =========================================================
def new_candidate(address: str) -> Dict[str, Any]:
    return {
        "address": address,
        "score": 0,
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
        "time_proximity_hits": 0,
        "similar_amount_hits": 0,

        "first_seen_ts": None,
        "last_seen_ts": None,
    }


def add_signal(candidate: Dict[str, Any], signal_type: str, score: int, details: str) -> None:
    candidate["score"] += score
    candidate["signals"].append({
        "type": signal_type,
        "score": score,
        "details": details,
    })
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


def estimate_contract_popularity_early(
    client: httpx.Client,
    network: str,
    contract_addr: str,
) -> Dict[str, Any]:
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

            if len(unique_senders) >= EARLY_VERY_POPULAR_SENDER_THRESHOLD:
                early_stopped = True
                break
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

            if len(unique_senders) >= EARLY_VERY_POPULAR_SENDER_THRESHOLD:
                early_stopped = True
                break
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


def estimate_time_proximity(
    target: str,
    candidate: str,
    target_txs: List[Dict[str, Any]],
    candidate_txs: List[Dict[str, Any]],
) -> Dict[str, int]:
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


def estimate_amount_similarity(
    target: str,
    candidate: str,
    target_txs: List[Dict[str, Any]],
    candidate_txs: List[Dict[str, Any]],
) -> int:
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


def fetch_txs_for_wallets(
    client: httpx.Client,
    network: str,
    wallets: List[str],
) -> Dict[str, List[Dict[str, Any]]]:
    result: Dict[str, List[Dict[str, Any]]] = {}
    for wallet in list(dict.fromkeys(wallets))[:MAX_CANDIDATES_PER_NETWORK]:
        txs = get_normal_transactions(
            client,
            wallet,
            network,
            max_pages_override=CANDIDATE_WALLET_MAX_PAGES,
            max_total_rows=CANDIDATE_WALLET_MAX_TXS,
        )

        if len(txs) >= CANDIDATE_WALLET_MAX_TXS:
            log(f"[tx_cache] candidate wallet capped {wallet} on {network}: tx_count={len(txs)}")

        result[wallet] = txs
    return result


def find_shared_funding_sources(
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

    target_sources = incoming_sources(target, network_txs_by_wallet.get(target, []))
    result: Dict[str, List[str]] = {}

    for wallet in candidate_wallets:
        wallet_sources = incoming_sources(wallet, network_txs_by_wallet.get(wallet, []))
        shared = sorted(target_sources & wallet_sources)
        if shared:
            result[wallet] = shared

    return result


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

        log(
            f"[popularity] {network} {contract_addr} "
            f"senders={meta['unique_senders_count']} "
            f"popularity={meta['popularity']} "
            f"early_stopped={meta['early_stopped']}"
        )

    return result


def build_candidate_scores(
    target: str,
    network: str,
    direct_wallets: Dict[str, Dict[str, Any]],
    exchange_cluster_wallets: Dict[str, Dict[str, Any]],
    shared_funding_sources: Dict[str, List[str]],
    contract_popularity_map: Dict[str, Dict[str, Any]],
    network_txs_by_wallet: Dict[str, List[Dict[str, Any]]],
) -> Dict[str, Dict[str, Any]]:
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

        add_signal(c, "direct_transfer", WEIGHT_DIRECT_ONCE, f"count={info['count']}")
        if info["repeat_transfer"]:
            add_signal(c, "repeat_transfer", WEIGHT_DIRECT_REPEAT, "count>1")
        if info.get("bidirectional"):
            add_signal(c, "bidirectional", WEIGHT_BIDIRECTIONAL, "in>0 and out>0")

    for addr, info in exchange_cluster_wallets.items():
        c = ensure(addr)
        for contract_addr in info.get("shared_exchange_addresses", []):
            meta = contract_popularity_map.get(contract_addr, {})
            popularity = meta.get("popularity", "medium")
            score = contract_popularity_score(popularity)
            c["shared_contracts"].append(contract_addr)
            add_signal(c, "shared_contract", score, f"{contract_addr} popularity={popularity}")

    for addr, sources in shared_funding_sources.items():
        c = ensure(addr)
        c["shared_funding_sources"] = list(sources)
        add_signal(c, "shared_funding_source", WEIGHT_SHARED_FUNDING_SOURCE, f"sources={len(sources)}")

    for addr, c in candidates.items():
        target_txs = network_txs_by_wallet.get(target, [])
        cand_txs = network_txs_by_wallet.get(addr, [])

        prox = estimate_time_proximity(target, addr, target_txs, cand_txs)
        c["time_proximity_hits"] = prox["hits_1h"] + prox["hits_24h"]
        if prox["hits_1h"] > 0:
            add_signal(c, "time_proximity_1h", WEIGHT_TIME_PROXIMITY_1H, f"hits={prox['hits_1h']}")
        elif prox["hits_24h"] > 0:
            add_signal(c, "time_proximity_24h", WEIGHT_TIME_PROXIMITY_24H, f"hits={prox['hits_24h']}")

        amount_hits = estimate_amount_similarity(target, addr, target_txs, cand_txs)
        c["similar_amount_hits"] = amount_hits
        if amount_hits > 0:
            add_signal(c, "similar_amount", WEIGHT_SIMILAR_AMOUNT, f"hits={amount_hits}")

    for c in candidates.values():
        finalize_candidate_confidence(c)

    return candidates


def split_candidates_by_confidence(
    candidates: Dict[str, Dict[str, Any]],
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    final_cluster: Dict[str, Dict[str, Any]] = {}
    possible_cluster: Dict[str, Dict[str, Any]] = {}

    for addr, c in candidates.items():
        if c["score"] >= FINAL_SCORE_THRESHOLD and len(c["signal_types"]) >= 2:
            final_cluster[addr] = c
        elif c["score"] >= POSSIBLE_SCORE_THRESHOLD:
            possible_cluster[addr] = c

    return final_cluster, possible_cluster


# =========================================================
# CHECKPOINT / RESUME
# =========================================================
def _checkpoint_path(target: str, date_from: str, date_to: str) -> str:
    os.makedirs(CHECKPOINT_DIR, exist_ok=True)
    key = hashlib.md5(f"{target}:{date_from[:10]}:{date_to[:10]}".encode()).hexdigest()[:8]
    return os.path.join(CHECKPOINT_DIR, f"{target}_{key}.json")


def _serialize_report(report: Dict[str, Any]) -> Dict[str, Any]:
    def _fix(info: Dict[str, Any]) -> Dict[str, Any]:
        out = dict(info)
        for k, v in list(out.items()):
            if isinstance(v, set):
                out[k] = sorted(v)
        return out

    return {
        "tx_count": report["tx_count"],
        "direct_wallets": {k: _fix(v) for k, v in report["direct_wallets"].items()},
        "exchange_cluster_wallets": {k: _fix(v) for k, v in report["exchange_cluster_wallets"].items()},
        "shared_exchanges": {k: _fix(v) for k, v in report["shared_exchanges"].items()},
        "candidate_scores": {k: _fix(v) for k, v in report.get("candidate_scores", {}).items()},
        "final_cluster_wallets": {k: _fix(v) for k, v in report.get("final_cluster_wallets", {}).items()},
        "possible_cluster_wallets": {k: _fix(v) for k, v in report.get("possible_cluster_wallets", {}).items()},
        "shared_funding_sources": report.get("shared_funding_sources", {}),
        "contract_popularity_map": report.get("contract_popularity_map", {}),
    }


def checkpoint_save(
    target: str,
    date_from: str,
    date_to: str,
    all_reports: Dict[str, Dict[str, Any]],
    contract_cache: Dict[Tuple[str, str], bool],
) -> None:
    path = _checkpoint_path(target, date_from, date_to)
    data = {
        "target": target,
        "date_from": date_from,
        "date_to": date_to,
        "completed_networks": list(all_reports.keys()),
        "reports": {net: _serialize_report(rep) for net, rep in all_reports.items()},
        "contract_cache": {f"{net}:{addr}": val for (net, addr), val in contract_cache.items()},
    }
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        log(f"[checkpoint] saved ({list(all_reports.keys())}) → {path}")
    except Exception as e:
        log(f"[checkpoint] save failed: {e}")


def checkpoint_load(
    target: str,
    date_from: str,
    date_to: str,
) -> Optional[Dict[str, Any]]:
    path = _checkpoint_path(target, date_from, date_to)
    if not os.path.exists(path):
        return None
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        if data.get("target") != target:
            return None
        log(f"[checkpoint] loaded for {target}: completed={data.get('completed_networks', [])}")
        return data
    except Exception as e:
        log(f"[checkpoint] load failed: {e}")
        return None


def checkpoint_clear(target: str, date_from: str, date_to: str) -> None:
    path = _checkpoint_path(target, date_from, date_to)
    try:
        if os.path.exists(path):
            os.remove(path)
            log(f"[checkpoint] cleared: {path}")
    except Exception as e:
        log(f"[checkpoint] clear failed: {e}")


def _restore_contract_cache(
    raw: Dict[str, bool],
) -> Dict[Tuple[str, str], bool]:
    cache: Dict[Tuple[str, str], bool] = {}
    for key, val in raw.items():
        parts = key.split(":", 1)
        if len(parts) == 2:
            cache[(parts[0], parts[1])] = bool(val)
    return cache


# =========================================================
# ETHOS
# =========================================================
def check_ethos_account(client: httpx.Client, address: str) -> Dict[str, Any]:
    url = f"{ETHOS_API_BASE}/api/v2/user/by/address/{address}"
    result = http_request_with_retries(
        client, "GET", url,
        op_name=f"ethos:by_address:{address}",
        max_retries=MAX_RETRIES,
        retry_on_statuses={408, 429, 500, 502, 503, 504},
    )
    if not result["ok"]:
        return {
            "address": address, "exists": None, "status": None,
            "username": None, "profile_url": None, "error": result.get("error"),
        }

    resp = result["response"]

    if resp.status_code == 404:
        return {
            "address": address, "exists": False, "status": None,
            "username": None, "profile_url": None, "error": None,
        }

    if resp.status_code != 200:
        return {
            "address": address, "exists": None, "status": None,
            "username": None, "profile_url": None,
            "error": f"unexpected status: {resp.status_code}",
        }

    try:
        data = resp.json()
    except Exception as e:
        return {
            "address": address, "exists": None, "status": None,
            "username": None, "profile_url": None,
            "error": f"invalid json: {e}",
        }

    links       = data.get("links") or {}
    username    = data.get("username")
    profile_url = links.get("profile")

    if not profile_url and username:
        profile_url = f"https://ethos.network/{username}"

    return {
        "address": address,
        "exists": True,
        "status": data.get("status"),
        "username": username,
        "profile_url": profile_url,
        "error": None,
    }


def check_ethos_accounts_for_addresses(
    client: httpx.Client,
    addresses: List[str],
) -> List[Dict[str, Any]]:
    unique_addresses = list(dict.fromkeys([a for a in addresses if a]))
    return [check_ethos_account(client, addr) for addr in unique_addresses]


def is_ethos_active(item: Dict[str, Any]) -> bool:
    if item.get("exists") is not True:
        return False
    if not item.get("profile_url"):
        return False
    status = item.get("status")
    if status is None:
        return False
    return str(status).strip().lower() == "active"


def build_final_ethos_summary(
    target: str,
    main_ethos: Dict[str, Any],
    linked_ethos_results: List[Dict[str, Any]],
    lang: str = "ru",
) -> str:
    active_linked = [x for x in linked_ethos_results if is_ethos_active(x)]

    lines: List[str] = [msg("ethos_summary_title", lang), ""]
    lines.append(msg("main_address_line", lang, target=target))

    if is_ethos_active(main_ethos):
        lines.append(
            f"   Ethos: {main_ethos['profile_url']} | "
            f"user={main_ethos.get('username') or 'n/a'} | "
            f"status={main_ethos.get('status') or 'n/a'}"
        )
    else:
        lines.append(msg("ethos_not_found", lang))

    lines.append("")

    if active_linked:
        lines.append(msg("linked_ethos_found", lang))
        for item in active_linked:
            lines.append(
                f"- {item['address']} | "
                f"user={item.get('username') or 'n/a'} | "
                f"status={item.get('status') or 'n/a'} | "
                f"{item['profile_url']}"
            )
    else:
        lines.append(msg("no_linked_ethos", lang))

    return "\n".join(lines)


# =========================================================
# ANALYSIS
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
        to_addr   = normalize_address(tx.get("to"))
        if not from_addr or not to_addr:
            continue
        if target not in (from_addr, to_addr):
            continue

        tx_hash   = tx.get("hash")
        ts        = str(tx.get("timeStamp", ""))
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
        to_addr   = normalize_address(tx.get("to"))
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


def find_wallets_sharing_exchanges_alchemy(
    client: httpx.Client,
    target: str,
    network: str,
    exchange_addresses: Dict[str, Dict[str, Any]],
    contract_cache: Dict[Tuple[str, str], bool],
    contract_popularity_map: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Dict[str, Dict[str, Any]]:
    candidates: Dict[str, List[Tuple[str, str]]] = {}

    limited = dict(list(exchange_addresses.items())[:MAX_EXCHANGE_CONTRACTS_TO_SCAN])
    total   = len(limited)

    for ex_idx, (ex_addr, ex_meta) in enumerate(limited.items(), 1):
        popularity_meta = (contract_popularity_map or {}).get(ex_addr, {})
        popularity = popularity_meta.get("popularity", "medium")

        if should_skip_deep_contract_scan(popularity_meta):
            log(f"[alchemy] {network} exchange scan skipped for massive contract {ex_addr} popularity={popularity}")
            continue

        log(f"[alchemy] {network} exchange scan {ex_idx}/{total}: {ex_addr} popularity={popularity}")
        txs = get_normal_transactions(
            client,
            ex_addr,
            network,
            max_pages_override=CONTRACT_POPULARITY_EARLY_PAGES + 3,
            max_total_rows=HUGE_ADDRESS_TX_THRESHOLD,
        )

        if SKIP_DEEP_SCAN_FOR_HUGE_ADDRESS and len(txs) >= HUGE_ADDRESS_TX_THRESHOLD:
            log(f"[alchemy] {network} skip huge contract {ex_addr}: tx_count={len(txs)}")
            continue

        added = 0
        for tx in txs:
            from_addr = normalize_address(tx.get("from"))
            to_addr   = normalize_address(tx.get("to"))
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
        bucket = clustered.setdefault(
            wallet,
            {
                "address": wallet,
                "shared_exchange_addresses": set(),
                "shared_exchange_labels": set(),
            },
        )
        for ex_addr, label in exchanges:
            bucket["shared_exchange_addresses"].add(ex_addr)
            bucket["shared_exchange_labels"].add(label)

    for data in clustered.values():
        data["shared_exchange_addresses"] = sorted(data["shared_exchange_addresses"])
        data["shared_exchange_labels"]    = sorted(data["shared_exchange_labels"])

    return clustered


def analyze_shared_exchanges_alchemy(
    client: httpx.Client,
    network: str,
    wallets: List[str],
    contract_cache: Dict[Tuple[str, str], bool],
) -> Dict[str, Dict[str, Any]]:
    unique_wallets = list(dict.fromkeys([w for w in wallets if w]))[:MAX_CLUSTER_WALLETS_FOR_EXCHANGE_CHECK]

    wallet_destinations: Dict[str, Set[str]] = {}
    all_destinations: Set[str] = set()

    for w_idx, wallet in enumerate(unique_wallets, 1):
        log(f"[alchemy] {network} shared-exchange scan wallet {w_idx}/{len(unique_wallets)}: {wallet}")
        txs = get_normal_transactions(
            client,
            wallet,
            network,
            max_pages_override=SHARED_SCAN_WALLET_MAX_PAGES,
            max_total_rows=SHARED_SCAN_WALLET_MAX_TXS,
        )

        if len(txs) >= SHARED_SCAN_WALLET_MAX_TXS:
            log(f"[alchemy] {network} shared scan capped wallet {wallet}: tx_count={len(txs)}")

        dests: Set[str] = set()
        for tx in txs:
            from_addr = normalize_address(tx.get("from"))
            to_addr   = normalize_address(tx.get("to"))
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
            bucket = exchange_map.setdefault(
                dest,
                {"exchange_address": dest, "label": "Contract", "source_wallets": set()},
            )
            bucket["source_wallets"].add(wallet)

    result: Dict[str, Dict[str, Any]] = {}
    for ex_addr, data in exchange_map.items():
        if len(data["source_wallets"]) >= 2:
            data["source_wallets"] = sorted(data["source_wallets"])
            result[ex_addr] = data
    return result


# =========================================================
# REPORT BUILDERS
# =========================================================
def build_scored_candidates_section(
    final_cluster_wallets: Dict[str, Dict[str, Any]],
    possible_cluster_wallets: Dict[str, Dict[str, Any]],
) -> str:
    lines: List[str] = []

    lines.append("5) Final scored cluster:")
    if not final_cluster_wallets:
        lines.append("- no high-confidence candidates")
    else:
        for addr, info in sorted(final_cluster_wallets.items(), key=lambda x: (-x[1]["score"], x[0]))[:REPORT_MAX_CLUSTER]:
            signal_names = ", ".join(sorted(info.get("signal_types", [])))
            lines.append(f"- {addr} | score={info['score']} | confidence={info['confidence']} | signals={signal_names}")

    lines.append("")
    lines.append("6) Possible candidates:")
    if not possible_cluster_wallets:
        lines.append("- none")
    else:
        for addr, info in sorted(possible_cluster_wallets.items(), key=lambda x: (-x[1]["score"], x[0]))[:REPORT_MAX_CLUSTER]:
            signal_names = ", ".join(sorted(info.get("signal_types", [])))
            lines.append(f"- {addr} | score={info['score']} | confidence={info['confidence']} | signals={signal_names}")

    return "\n".join(lines)


def build_network_report(
    target: str,
    network: str,
    tx_count: int,
    direct_wallets: Dict[str, Dict[str, Any]],
    exchange_cluster_wallets: Dict[str, Dict[str, Any]],
    shared_exchanges: Dict[str, Dict[str, Any]],
    final_cluster_wallets: Dict[str, Dict[str, Any]],
    possible_cluster_wallets: Dict[str, Dict[str, Any]],
    lang: str = "ru",
) -> str:
    lines: List[str] = [
        f"=== {network.upper()} ===",
        f"Target: {target}",
        f"Normal tx fetched: {tx_count}",
        "",
    ]

    if not direct_wallets:
        lines.append(msg("no_direct_transfers", lang))
    else:
        sorted_direct = sorted(direct_wallets.items(), key=lambda x: (-x[1]["count"], x[0]))
        shown_direct  = sorted_direct[:REPORT_MAX_DIRECT]
        lines.append(msg("direct_transfers_found", lang))
        for addr, info in shown_direct:
            repeat_mark = " [REPEAT]" if info["repeat_transfer"] else ""
            first_ts = ts_to_human(info["timestamps"][-1]) if info["timestamps"] else "n/a"
            last_ts  = ts_to_human(info["timestamps"][0])  if info["timestamps"] else "n/a"
            bi = " | bidirectional=yes" if info.get("bidirectional") else ""
            lines.append(
                f"- {addr}{repeat_mark} | total={info['count']} | in={info['incoming_count']} | "
                f"out={info['outgoing_count']} | first={first_ts} | last={last_ts}{bi}"
            )
        if len(sorted_direct) > REPORT_MAX_DIRECT:
            lines.append(f"  ... и ещё {len(sorted_direct) - REPORT_MAX_DIRECT} адресов")

    lines.append("")
    if not exchange_cluster_wallets:
        lines.append(msg("no_exchange_cluster", lang))
    else:
        sorted_cluster = sorted(exchange_cluster_wallets.items())
        shown_cluster  = sorted_cluster[:REPORT_MAX_CLUSTER]
        lines.append(msg("exchange_cluster_found", lang))
        for addr, info in shown_cluster:
            exch = ", ".join(info.get("shared_exchange_addresses", []))
            lines.append(f"- {addr} | shared_contract={exch}")
        if len(sorted_cluster) > REPORT_MAX_CLUSTER:
            lines.append(f"  ... и ещё {len(sorted_cluster) - REPORT_MAX_CLUSTER} адресов")

    lines.append("")
    repeated = [x["address"] for x in direct_wallets.values() if x["repeat_transfer"]]
    if not repeated:
        lines.append(msg("no_repeated_transfers", lang))
    else:
        lines.append(msg("repeated_transfers_found", lang))
        for addr in repeated[:REPORT_MAX_DIRECT]:
            lines.append(f"- {addr}")
        if len(repeated) > REPORT_MAX_DIRECT:
            lines.append(f"  ... и ещё {len(repeated) - REPORT_MAX_DIRECT}")

    lines.append("")
    if not shared_exchanges:
        lines.append(msg("no_shared_exchanges", lang))
    else:
        sorted_shared = sorted(shared_exchanges.items(), key=lambda x: (-len(x[1]["source_wallets"]), x[0]))
        shown_shared = sorted_shared[:REPORT_MAX_SHARED]
        lines.append(msg("shared_exchanges_found", lang))
        for ex_addr, data in shown_shared:
            wallets = list(data["source_wallets"])
            lines.append(f"- {ex_addr} | label={data['label']} | wallets={len(wallets)}")
            for w in wallets[:REPORT_MAX_WALLETS_PER_EXCHANGE]:
                lines.append(f"    • {w}")
            if len(wallets) > REPORT_MAX_WALLETS_PER_EXCHANGE:
                lines.append(f"    ... +{len(wallets) - REPORT_MAX_WALLETS_PER_EXCHANGE}")
        if len(sorted_shared) > REPORT_MAX_SHARED:
            lines.append(f"  ... и ещё {len(sorted_shared) - REPORT_MAX_SHARED} общих контрактных адресов")

    lines.append("")
    lines.append(build_scored_candidates_section(final_cluster_wallets, possible_cluster_wallets))
    lines.append("")
    return "\n".join(lines)


def build_summary(all_reports: Dict[str, Dict[str, Any]], lang: str = "ru") -> str:
    lines = [msg("final_summary_title", lang)]
    for network, data in all_reports.items():
        direct_wallets = data["direct_wallets"]
        exchange_cluster_wallets = data["exchange_cluster_wallets"]
        shared_exchanges = data["shared_exchanges"]
        final_cluster_wallets = data.get("final_cluster_wallets", {})
        possible_cluster_wallets = data.get("possible_cluster_wallets", {})
        repeated = sum(1 for x in direct_wallets.values() if x["repeat_transfer"])
        raw_cluster_size = len(set(direct_wallets.keys()) | set(exchange_cluster_wallets.keys()))
        lines.append(
            f"- {network}: raw_cluster={raw_cluster_size}, final={len(final_cluster_wallets)}, "
            f"possible={len(possible_cluster_wallets)}, direct_eoa={len(direct_wallets)}, "
            f"repeated={repeated}, shared_contracts={len(shared_exchanges)}"
        )
    return "\n".join(lines)


def build_address_map_message(
    target: str,
    all_reports: Dict[str, Dict[str, Any]],
    lang: str = "ru",
) -> str:
    strong_addresses: Set[str] = set()
    possible_addresses: Set[str] = set()
    exchange_addresses: Dict[str, str] = {}

    for network_data in all_reports.values():
        for addr in network_data.get("final_cluster_wallets", {}):
            if addr != target:
                strong_addresses.add(addr)
        for addr in network_data.get("possible_cluster_wallets", {}):
            if addr != target and addr not in strong_addresses:
                possible_addresses.add(addr)
        for ex_addr, ex_data in network_data.get("shared_exchanges", {}).items():
            exchange_addresses[ex_addr] = ex_data.get("label", "Contract")

    lines: List[str] = [
        msg("address_map_title", lang), "",
        msg("main_address_map_line", lang, target=target), "",
    ]

    if strong_addresses:
        lines.append(msg("cluster_addresses", lang))
        for addr in sorted(strong_addresses):
            lines.append(f"🔴 {addr}")
    else:
        lines.append(msg("no_cluster_addresses", lang))

    lines.append("")

    if possible_addresses:
        lines.append(msg("possible_cluster_addresses", lang))
        for addr in sorted(possible_addresses):
            lines.append(f"🟡 {addr}")
    else:
        lines.append(msg("no_possible_cluster_addresses", lang))

    lines.append("")

    if exchange_addresses:
        lines.append(msg("shared_exchange_addresses", lang))
        for addr, label in sorted(exchange_addresses.items()):
            lines.append(f"🟠 {addr} — {label}")
    else:
        lines.append(msg("no_shared_exchange_addresses", lang))

    return "\n".join(lines)


def collect_related_addresses(
    target: str,
    all_reports: Dict[str, Dict[str, Any]],
) -> List[str]:
    related: Set[str] = set()
    for network_data in all_reports.values():
        for addr in network_data.get("final_cluster_wallets", {}):
            if addr != target:
                related.add(addr)
    return sorted(related)


# =========================================================
# MAIN WALLET ANALYSIS
# =========================================================
def analyze_wallet(client: httpx.Client, target: str, chat_id: str) -> None:
    lang = get_lang(chat_id)

    date_to_iso   = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    date_from_iso = (datetime.now(timezone.utc) - timedelta(days=180)).strftime("%Y-%m-%dT%H:%M:%SZ")

    ckpt = checkpoint_load(target, date_from_iso, date_to_iso)
    if ckpt:
        all_reports: Dict[str, Dict[str, Any]] = ckpt.get("reports", {})
        contract_cache: Dict[Tuple[str, str], bool] = _restore_contract_cache(ckpt.get("contract_cache", {}))
        completed = set(ckpt.get("completed_networks", []))
        remaining = [n for n in DEFAULT_NETWORKS if n not in completed]
        send_telegram_message(
            client, chat_id,
            f"↩️ Продолжаю с чекпоинта. Уже готово: {', '.join(sorted(completed))}. "
            f"Осталось: {', '.join(remaining) or 'только финальный отчёт'}",
        )
    else:
        all_reports = {}
        contract_cache = {}
        completed = set()
        send_telegram_message(
            client, chat_id,
            msg("start_analysis", lang, target=target, networks=", ".join(DEFAULT_NETWORKS)),
        )

    for network in DEFAULT_NETWORKS:
        if network in completed:
            log(f"[checkpoint] skipping already-completed network: {network}")
            continue

        _check_cancelled(chat_id)
        send_telegram_message(client, chat_id, msg("scanning_network", lang, network=network))

        log(f"[{network}] step 1/7: fetching target transactions...")
        txs = get_normal_transactions(client, target, network)
        log(f"[{network}] step 1/7 done: {len(txs)} txs")
        _check_cancelled(chat_id)

        log(f"[{network}] step 2/7: analyzing direct transfers...")
        direct_wallets = analyze_direct_wallet_transfers(
            client=client,
            target=target,
            network=network,
            txs=txs,
            contract_cache=contract_cache,
        )
        log(f"[{network}] step 2/7 done: {len(direct_wallets)} direct EOA wallets found")
        _check_cancelled(chat_id)

        log(f"[{network}] step 3/7: collecting target contract destinations...")
        target_exchange_addresses = get_exchange_addresses_from_txs(
            target=target,
            network=network,
            txs=txs,
            contract_cache=contract_cache,
        )
        log(f"[{network}] step 3/7 done: {len(target_exchange_addresses)} target contracts found")
        _check_cancelled(chat_id)

        log(f"[{network}] step 4/7: estimating contract popularity...")
        contract_popularity_map = build_contract_popularity_map(
            client=client,
            network=network,
            exchange_addresses=target_exchange_addresses,
        )
        log(f"[{network}] step 4/7 done: popularity estimated for {len(contract_popularity_map)} contracts")
        _check_cancelled(chat_id)

        log(f"[{network}] step 5/7: scanning wallets sharing target contracts (skip massive contracts)...")
        exchange_cluster_wallets = find_wallets_sharing_exchanges_alchemy(
            client=client,
            target=target,
            network=network,
            exchange_addresses=target_exchange_addresses,
            contract_cache=contract_cache,
            contract_popularity_map=contract_popularity_map,
        )
        exchange_cluster_wallets = {
            addr: info
            for addr, info in exchange_cluster_wallets.items()
            if addr not in direct_wallets
        }
        log(f"[{network}] step 5/7 done: {len(exchange_cluster_wallets)} raw exchange-cluster wallets")
        _check_cancelled(chat_id)

        candidate_wallets = sorted(set(direct_wallets.keys()) | set(exchange_cluster_wallets.keys()))
        limited_candidate_wallets = candidate_wallets[:MAX_CANDIDATES_PER_NETWORK]

        log(f"[{network}] step 6/7: building candidate tx cache + funding overlaps...")
        network_txs_by_wallet = {target: txs}
        network_txs_by_wallet.update(fetch_txs_for_wallets(client, network, limited_candidate_wallets))

        shared_funding_sources = find_shared_funding_sources(
            target=target,
            candidate_wallets=limited_candidate_wallets,
            network_txs_by_wallet=network_txs_by_wallet,
            contract_cache=contract_cache,
            network=network,
        )
        log(f"[{network}] step 6/7 done: funding overlaps for {len(shared_funding_sources)} wallets")
        _check_cancelled(chat_id)

        log(f"[{network}] step 7/7: scoring candidates...")
        candidate_scores = build_candidate_scores(
            target=target,
            network=network,
            direct_wallets=direct_wallets,
            exchange_cluster_wallets=exchange_cluster_wallets,
            shared_funding_sources=shared_funding_sources,
            contract_popularity_map=contract_popularity_map,
            network_txs_by_wallet=network_txs_by_wallet,
        )
        final_cluster_wallets, possible_cluster_wallets = split_candidates_by_confidence(candidate_scores)
        log(f"[{network}] step 7/7 done: final={len(final_cluster_wallets)} possible={len(possible_cluster_wallets)}")
        _check_cancelled(chat_id)

        cluster_wallets = [target] + sorted(final_cluster_wallets.keys())

        log(f"[{network}] shared-contract scan for {len(cluster_wallets)} final-cluster wallets...")
        shared_exchanges = analyze_shared_exchanges_alchemy(
            client=client,
            network=network,
            wallets=cluster_wallets,
            contract_cache=contract_cache,
        )
        log(f"[{network}] shared-contract scan done: {len(shared_exchanges)} shared contracts")

        all_reports[network] = {
            "tx_count": len(txs),
            "direct_wallets": direct_wallets,
            "exchange_cluster_wallets": exchange_cluster_wallets,
            "shared_exchanges": shared_exchanges,
            "candidate_scores": candidate_scores,
            "final_cluster_wallets": final_cluster_wallets,
            "possible_cluster_wallets": possible_cluster_wallets,
            "shared_funding_sources": shared_funding_sources,
            "contract_popularity_map": contract_popularity_map,
        }

        checkpoint_save(target, date_from_iso, date_to_iso, all_reports, contract_cache)

        send_telegram_message(
            client, chat_id,
            build_network_report(
                target=target,
                network=network,
                tx_count=len(txs),
                direct_wallets=direct_wallets,
                exchange_cluster_wallets=exchange_cluster_wallets,
                shared_exchanges=shared_exchanges,
                final_cluster_wallets=final_cluster_wallets,
                possible_cluster_wallets=possible_cluster_wallets,
                lang=lang,
            ),
        )

    send_telegram_message(client, chat_id, build_summary(all_reports, lang=lang))
    send_telegram_message(client, chat_id, build_address_map_message(target, all_reports, lang=lang))

    checkpoint_clear(target, date_from_iso, date_to_iso)

    related_addresses = collect_related_addresses(target, all_reports)
    send_telegram_message(client, chat_id, msg("ethos_stage", lang, n=len(related_addresses)))

    main_ethos = check_ethos_account(client, target)
    linked_ethos_results = check_ethos_accounts_for_addresses(client, related_addresses)

    send_telegram_message(
        client, chat_id,
        build_final_ethos_summary(
            target=target,
            main_ethos=main_ethos,
            linked_ethos_results=linked_ethos_results,
            lang=lang,
        ),
    )


# =========================================================
# TELEGRAM MESSAGE ROUTER
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
    chat    = message.get("chat", {})
    chat_id = str(chat.get("id"))
    text    = (message.get("text") or "").strip()

    if not chat_id:
        return

    if TELEGRAM_ALLOWED_CHAT_ID and str(TELEGRAM_ALLOWED_CHAT_ID) != chat_id:
        log(f"Ignored message from unauthorized chat_id={chat_id}")
        return

    lang = get_lang(chat_id)

    if text == "/cancel":
        with _threads_lock:
            thread = _active_threads.get(chat_id)
            event  = _cancel_events.get(chat_id)
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
                    offset  = upd["update_id"] + 1
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
