import io
import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from fake_useragent import UserAgent
from minio import Minio
from minio.error import S3Error

SRC_DIR = Path(__file__).resolve().parents[2] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from core.config import get_settings

logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(handler)
logger.setLevel(logging.INFO)

settings = get_settings()
ua = UserAgent()
API = settings.api_historical_url
BUCKET_NAME = settings.minio_bucket_raw_price
_endpoint = settings.minio_endpoint
MINIO_ENDPOINT = _endpoint.replace("http://", "").replace("https://", "")
MINIO_ACCESS_KEY = settings.minio_access_key
MINIO_SECRET_KEY = settings.minio_secret_key
MINIO_SECURE = _endpoint.startswith("https://")

OBJECT_FOLDER = "historical"
OBJECT_NAME = f"{OBJECT_FOLDER}/historical_data.json"

RATE_LIMIT_SECONDS = 20


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
    except Exception:
        pass

    df = df.astype(object).where(pd.notnull(df), None)
    return df


def _parse_any_json(raw: str):
    try:
        data = json.loads(raw)
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            return [data]
    except Exception:
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
        if isinstance(obj, list):
            out.extend(obj)
        elif isinstance(obj, dict):
            out.append(obj)
    return out


def get_data(coin_data, vs_currency="usd", days=365):
    if isinstance(coin_data, dict):
        coin_id = (
            coin_data.get("nameid") or coin_data.get("id") or coin_data.get("symbol")
        )
    else:
        coin_id = coin_data

    if not coin_id:
        logger.warning("No coin_id found in coin_data: %s", coin_data)
        return None

    try:
        url_ohlc = f"{API}/coins/{coin_id}/ohlc"
        resp_ohlc = requests.get(
            url_ohlc,
            params={"vs_currency": vs_currency, "days": int(days)},
            headers={"User-Agent": ua.random},
            timeout=60,
        )
        resp_ohlc.raise_for_status()
        ohlc_data = resp_ohlc.json()
        if not ohlc_data:
            logger.info("No OHLC data for %s (days=%s)", coin_id, days)
            return None
        df_ohlc = pd.DataFrame(
            ohlc_data, columns=["timestamp", "open", "high", "low", "close"]
        )

        url_mc = f"{API}/coins/{coin_id}/market_chart"
        resp_mc = requests.get(
            url_mc,
            params={"vs_currency": vs_currency, "days": int(days)},
            headers={"User-Agent": ua.random},
            timeout=60,
        )
        resp_mc.raise_for_status()
        mc_data = resp_mc.json()
        if not mc_data or "total_volumes" not in mc_data:
            logger.info("No market_chart data for %s (days=%s)", coin_id, days)
            return None
        df_vol = pd.DataFrame(mc_data["total_volumes"], columns=["timestamp", "volume"])

        df = pd.merge(df_ohlc, df_vol, on="timestamp", how="left")
        df["datetime"] = pd.to_datetime(df["timestamp"], unit="ms").dt.strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        df["coin_id"] = coin_id
        df = df[["coin_id", "datetime", "open", "high", "low", "close", "volume"]]

        return df

    except requests.exceptions.RequestException as e:
        logger.error("HTTP error while fetching %s: %s", coin_id, e)
        return None
    except Exception as e:
        logger.exception("Unexpected error while fetching %s: %s", coin_id, e)
        return None


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
    coins_list = settings.coins
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
    json_str = json.dumps(result_df.to_dict(orient="records"), ensure_ascii=False)
    return json_str


def save_historical_to_minio(**context):
    ti = context.get("ti")
    if not ti:
        logger.error("No task instance found in context")
        return

    new_json = ti.xcom_pull(task_ids="fetch_historical_data")
    if not new_json:
        logger.error("No data retrieved from XCom")
        return

    try:
        new_df = pd.DataFrame(json.loads(new_json))
    except Exception:
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
            except Exception:
                pass

        combined_df = (
            pd.concat([old_df, new_df], ignore_index=True)
            if not old_df.empty
            else new_df.copy()
        )
        combined_df.drop_duplicates(subset=["coin_id", "datetime"], inplace=True)
        combined_df.sort_values(by=["coin_id", "datetime"], inplace=True)

        combined_df = _clean_df_for_json(combined_df)

        records = combined_df.to_dict(orient="records")
        json_bytes = json.dumps(
            records, ensure_ascii=False, indent=2, allow_nan=False
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
        logger.exception("Failed to save historical to MinIO: %s", e)
