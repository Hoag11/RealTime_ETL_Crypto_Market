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

# ============================================
# CONFIG
# ============================================

ua = UserAgent()

BUCKET_NAME = configs.MINIO_BUCKET_RAW_PRICE
MINIO_ENDPOINT = configs.MINIO_ENDPOINT.replace("http://", "")
MINIO_ACCESS_KEY = configs.MINIO_ACCESS_KEY
MINIO_SECRET_KEY = configs.MINIO_SECRET_KEY
MINIO_SECURE = False

OBJECT_FOLDER = "historical"
OBJECT_NAME = f"{OBJECT_FOLDER}/historical_data.json"

API_URL = "https://min-api.cryptocompare.com/data/v2/histoday"
RATE_LIMIT_SECONDS = 1.0

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ============================================
# UTILS
# ============================================

def init_minio_client():
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )


def safe_request(url, params=None, max_retries=5):
    for attempt in range(max_retries):
        try:
            r = requests.get(
                url,
                params=params,
                headers={"User-Agent": ua.random},
                timeout=60,
            )

            if r.status_code == 429:
                wait = 2 ** attempt
                logging.warning(f"[429] Rate limit → sleep {wait}s")
                time.sleep(wait)
                continue

            r.raise_for_status()
            return r

        except Exception as e:
            wait = 2 ** attempt
            logging.warning(f"Request error ({e}) → retry {wait}s")
            time.sleep(wait)

    return None


def parse_any_json(raw: str):
    try:
        data = json.loads(raw)
        return data if isinstance(data, list) else [data]
    except:
        return []


def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df[["coin_id", "datetime", "open", "high", "low", "close", "volume"]]
    df["datetime"] = pd.to_datetime(df["datetime"]).dt.strftime("%Y-%m-%d %H:%M:%S")
    df = df.astype(object).where(pd.notnull(df), None)
    return df


def get_last_datetime_by_coin(old_df: pd.DataFrame) -> dict:
    if old_df.empty:
        return {}
    old_df["datetime"] = pd.to_datetime(old_df["datetime"])
    return old_df.groupby("coin_id")["datetime"].max().to_dict()

# ============================================
# CRAWL (LOGIC GIỐNG crawl.py)
# ============================================

def crawl_symbol(symbol: str) -> pd.DataFrame | None:
    logging.info(f"[FETCH] {symbol}")

    params = {
        "fsym": symbol,
        "tsym": "USD",
        "limit": 2000,
    }

    resp = safe_request(API_URL, params)
    time.sleep(RATE_LIMIT_SECONDS)

    if not resp:
        logging.error(f"[{symbol}] request failed")
        return None

    js = resp.json()
    if js.get("Response") != "Success":
        logging.error(f"[{symbol}] API error: {js.get('Message')}")
        return None

    data = js["Data"]["Data"]
    if not data:
        return None

    rows = []
    for row in data:
        # giống crawl.py
        dt = datetime.utcfromtimestamp(row["time"])

        # chặn dữ liệu rác
        if (
            row["open"] <= 0
            or row["high"] <= 0
            or row["low"] <= 0
            or row["close"] <= 0
        ):
            continue

        rows.append({
            "coin_id": symbol.lower(),
            "datetime": dt,
            "open": row["open"],
            "high": row["high"],
            "low": row["low"],
            "close": row["close"],
            "volume": row["volumeto"],
        })

    if not rows:
        return None

    return pd.DataFrame(rows)

# ============================================
# AIRFLOW TASKS
# ============================================

def fetch_historical_data(**context):
    client = init_minio_client()

    # Load old data
    old_df = pd.DataFrame()
    try:
        resp = client.get_object(BUCKET_NAME, OBJECT_NAME)
        old_raw = resp.read().decode("utf-8")
        old_df = pd.DataFrame(parse_any_json(old_raw))
    except:
        pass

    last_dt_map = get_last_datetime_by_coin(old_df)

    all_frames = []
    for symbol in configs.COINS:
        df = crawl_symbol(symbol)
        if df is None or df.empty:
            continue

        coin_id = symbol.lower()
        last_dt = last_dt_map.get(coin_id)

        if last_dt is not None:
            df = df[df["datetime"] > last_dt]

        if df.empty:
            logging.info(f"[SKIP] {symbol} no new data")
            continue

        all_frames.append(df)

    if not all_frames:
        logging.info("No new data → skip save")
        return "[]"

    result_df = pd.concat(all_frames, ignore_index=True)
    result_df = clean_df(result_df)

    return json.dumps(
        result_df.to_dict(orient="records"),
        ensure_ascii=False
    )


def save_historical_to_minio(**context):
    ti = context["ti"]
    new_json = ti.xcom_pull(task_ids="fetch_historical_data")

    if not new_json or new_json == "[]":
        return

    new_df = pd.DataFrame(json.loads(new_json))
    if new_df.empty:
        return

    new_df = clean_df(new_df)

    client = init_minio_client()

    old_df = pd.DataFrame()
    try:
        resp = client.get_object(BUCKET_NAME, OBJECT_NAME)
        old_raw = resp.read().decode("utf-8")
        old_df = pd.DataFrame(parse_any_json(old_raw))
    except:
        pass

    combined_df = (
        pd.concat([old_df, new_df], ignore_index=True)
        if not old_df.empty
        else new_df.copy()
    )

    combined_df.drop_duplicates(
        subset=["coin_id", "datetime"],
        keep="last",
        inplace=True
    )

    combined_df.sort_values(by=["coin_id", "datetime"], inplace=True)
    combined_df = clean_df(combined_df)

    json_bytes = json.dumps(
        combined_df.to_dict(orient="records"),
        ensure_ascii=False,
        indent=2,
        allow_nan=False,
    ).encode("utf-8")

    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)

    client.put_object(
        BUCKET_NAME,
        OBJECT_NAME,
        data=io.BytesIO(json_bytes),
        length=len(json_bytes),
        content_type="application/json",
    )
