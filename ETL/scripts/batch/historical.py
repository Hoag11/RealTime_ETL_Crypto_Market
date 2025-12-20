import io
import json
import time
import logging
import requests
import pandas as pd
from datetime import datetime
from fake_useragent import UserAgent
from minio import Minio
from minio.error import S3Error

from configs import configs

ua = UserAgent()
API = getattr(configs, "API_HISTORICAL", "https://api.coingecko.com/api/v3")
BUCKET_NAME = getattr(configs, "MINIO_BUCKET_RAW_PRICE", None)
MINIO_ENDPOINT = configs.MINIO_ENDPOINT.replace("http://", "")
MINIO_ACCESS_KEY = configs.MINIO_ACCESS_KEY
MINIO_SECRET_KEY = configs.MINIO_SECRET_KEY
MINIO_SECURE = False

OBJECT_FOLDER = "historical"
OBJECT_NAME = f"{OBJECT_FOLDER}/historical_data.json"

RATE_LIMIT_SECONDS = 20
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# ============================================
#   LOAD VALID COIN IDs (CoinGecko Validation)
# ============================================
def load_valid_coin_ids():
    url = f"{API}/coins/list"
    try:
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        valid = {item["id"] for item in data}
        logging.info(f"Loaded {len(valid)} valid CoinGecko IDs")
        return valid
    except Exception as e:
        logging.error(f"Failed to load coin list: {e}")
        return set()

VALID_COINS = load_valid_coin_ids()

# ============================================
#   SAFE REQUEST WITH RETRY + BACKOFF
# ============================================
def safe_request(url, params=None, max_retries=5):
    for attempt in range(max_retries):
        try:
            resp = requests.get(
                url,
                params=params,
                headers={"User-Agent": ua.random},
                timeout=60,
            )

            if resp.status_code == 429:
                # Rate limited → exponential backoff
                wait = 2 ** attempt
                logging.warning(f"[429] Rate-limit. Waiting {wait}s → retry...")
                time.sleep(wait)
                continue

            resp.raise_for_status()
            return resp

        except Exception as e:
            wait = 2 ** attempt
            logging.warning(f"Request failed ({e}). Retry in {wait}s...")
            time.sleep(wait)

    logging.error(f"Max retries exceeded for URL: {url}")
    return None

# ============================================
#   ORIGINAL CLEANING + JSON PARSE — KEEP SAME
# ============================================
def _init_minio_client():
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )


def _clean_df_for_json(df):
    cols = ["coin_id", "datetime", "open", "high", "low", "close", "volume"]
    for c in cols:
        if c not in df.columns:
            df[c] = None
    df = df[cols]

    try:
        df["datetime"] = pd.to_datetime(df["datetime"]).dt.strftime("%Y-%m-%d %H:%M:%S")
    except:
        pass

    df = df.astype(object).where(pd.notnull(df), None)
    return df


def _parse_any_json(raw: str):
    try:
        data = json.loads(raw)
        return data if isinstance(data, list) else [data]
    except:
        pass

    dec = json.JSONDecoder()
    i, n = 0, len(raw)
    out = []
    while i < n:
        while i < n and raw[i].isspace():
            i += 1
        if i >= n:
            break
        obj, idx = dec.raw_decode(raw, i)
        i = idx
        out.extend(obj if isinstance(obj, list) else [obj])
    return out

# ============================================
#   UPDATED get_data() WITH VALIDATION + RETRY
# ============================================
# ============================================
#   NEW get_data() USING CRYPTOCOMPARE API
# ============================================
def get_data(coin_data, vs_currency="usd", days=365):
    """
    Thay thế toàn bộ logic CoinGecko bằng CryptoCompare histoday API.
    Lấy full daily OHLCV, sau đó lọc ra đúng dữ liệu năm 2024.
    """

    # Detect symbol (CryptoCompare cần symbol, không phải id)
    if isinstance(coin_data, dict):
        symbol = coin_data.get("symbol") or coin_data.get("id") or coin_data.get("name")
    else:
        symbol = coin_data

    if not symbol:
        logging.warning("Missing coin symbol → skip")
        return None

    # chuẩn hóa symbol thành uppercase (CryptoCompare format)
    symbol = symbol.upper()

    logging.info(f"[CryptoCompare] Fetching OHLC for {symbol}...")

    # CryptoCompare daily historical endpoint
    url = "https://min-api.cryptocompare.com/data/v2/histoday"
    params = {
        "fsym": symbol,
        "tsym": "USD",
        "limit": 2000,   # full historical
    }

    resp = safe_request(url, params=params)
    if not resp:
        logging.error(f"[{symbol}] Request failed!")
        return None

    js = resp.json()

    if js.get("Response") != "Success":
        logging.error(f"[{symbol}] CryptoCompare error: {js}")
        return None

    data = js["Data"]["Data"]
    if not data:
        logging.warning(f"[{symbol}] No historical data returned.")
        return None

    # Convert -> DataFrame
    df = pd.DataFrame(data)

    # timestamp -> datetime
    df["datetime"] = pd.to_datetime(df["time"], unit="s")

    # Rename columns về schema hệ thống
    df = df.rename(
        columns={
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "volumeto": "volume",   # CryptoCompare uses "volumeto"
        }
    )

    # coin_id dùng symbol để đảm bảo consistency (vd: BTC, ETH)
    df["coin_id"] = symbol.lower()

    # giữ đúng format schema hệ thống
    df = df[
        ["coin_id", "datetime", "open", "high", "low", "close", "volume"]
    ]

    logging.info(f"[{symbol}] Loaded {len(df)} rows.")

    return df


# ============================================
#   REMAINDER OF FILE — KEEP ORIGINAL LOGIC
# ============================================
def fetch_historical_data(**context):
    client = _init_minio_client()
    bucket_name = BUCKET_NAME
    object_name = OBJECT_NAME

    first_run = True
    try:
        client.stat_object(bucket_name, object_name)
        first_run = False
    except S3Error:
        first_run = True

    all_frames = []
    coins_list = getattr(configs, "COINS", [])
    if not coins_list:
        return "[]"

    for idx, coin in enumerate(coins_list, start=1):
        days = 365 if first_run else 1
        df = get_data(coin, vs_currency="usd", days=days)
        if df is not None and not df.empty:
            all_frames.append(df)

        time.sleep(RATE_LIMIT_SECONDS)

    if not all_frames:
        return "[]"

    result_df = pd.concat(all_frames, ignore_index=True)
    result_df = _clean_df_for_json(result_df)
    return json.dumps(result_df.to_dict(orient="records"), ensure_ascii=False)


def save_historical_to_minio(**context):
    ti = context.get("ti")
    if not ti:
        return

    new_json = ti.xcom_pull(task_ids="fetch_historical_data")
    if not new_json:
        return

    try:
        new_df = pd.DataFrame(json.loads(new_json))
    except:
        return

    if new_df.empty:
        return

    new_df = _clean_df_for_json(new_df)

    client = _init_minio_client()
    bucket_name = BUCKET_NAME
    object_name = OBJECT_NAME

    try:
        old_df = pd.DataFrame()
        try:
            resp = client.get_object(bucket_name, object_name)
            old_raw = resp.read().decode("utf-8")
            old_records = _parse_any_json(old_raw)
            old_df = pd.DataFrame(old_records)
        finally:
            try:
                resp.close()
                resp.release_conn()
            except:
                pass

        combined_df = (
            pd.concat([old_df, new_df], ignore_index=True)
            if not old_df.empty
            else new_df.copy()
        )
        combined_df.drop_duplicates(subset=["coin_id", "datetime"], inplace=True)
        combined_df.sort_values(by=["coin_id", "datetime"], inplace=True)

        combined_df = _clean_df_for_json(combined_df)

        json_bytes = json.dumps(
            combined_df.to_dict(orient="records"),
            ensure_ascii=False,
            indent=2,
            allow_nan=False,
        ).encode("utf-8")

        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)

        client.put_object(
            bucket_name,
            object_name,
            data=io.BytesIO(json_bytes),
            length=len(json_bytes),
            content_type="application/json",
        )

    except Exception as e:
        logging.exception("Failed to save historical to MinIO: %s", e)
