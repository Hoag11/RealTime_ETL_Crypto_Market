import json
import logging
import os
import sys
import time
from datetime import datetime, timezone

import pandas as pd
import requests
from fake_useragent import UserAgent
from minio.error import S3Error


CURRENT_DIR = os.path.dirname(__file__)
SCRIPTS_DIR = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPTS_DIR, ".."))

for path in (PROJECT_ROOT, SCRIPTS_DIR):
    if path not in sys.path:
        sys.path.insert(0, path)


from configs import configs
from scripts.helpers.minio_utils import (
    bucket_has_objects,
    ensure_bucket,
    get_minio_client,
    upload_json_bytes,
)

ua = UserAgent()
API = getattr(configs, "API_HISTORICAL", "https://api.coingecko.com/api/v3")
BUCKET_NAME = getattr(configs, "MINIO_BUCKET_RAW_PRICE", None)
MINIO_ENDPOINT = configs.MINIO_ENDPOINT
MINIO_ACCESS_KEY = configs.MINIO_ACCESS_KEY
MINIO_SECRET_KEY = configs.MINIO_SECRET_KEY
MINIO_SECURE = False

OBJECT_FOLDER = "historical"

RATE_LIMIT_SECONDS = 20
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def _init_minio_client():
    return get_minio_client(
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


def get_data(coin_data, vs_currency="usd", days=365):
    if isinstance(coin_data, dict):
        coin_id = (
            coin_data.get("nameid") or coin_data.get("id") or coin_data.get("symbol")
        )
    else:
        coin_id = coin_data

    if not coin_id:
        logging.warning("No coin_id found in coin_data: %s", coin_data)
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
            logging.info("No OHLC data for %s (days=%s)", coin_id, days)
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
            logging.info("No market_chart data for %s (days=%s)", coin_id, days)
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
        logging.error("HTTP error while fetching %s: %s", coin_id, e)
        return None
    except Exception as e:
        logging.exception("Unexpected error while fetching %s: %s", coin_id, e)
        return None


def fetch_historical_data(**context):
    client = _init_minio_client()
    bucket_name = BUCKET_NAME

    first_run = True
    try:
        first_run = not bucket_has_objects(
            client, bucket_name, prefix=f"{OBJECT_FOLDER}/"
        )
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
    json_str = json.dumps(result_df.to_dict(orient="records"), ensure_ascii=False)
    return json_str


def save_historical_to_minio(**context):
    ti = context.get("ti")
    if not ti:
        return

    new_json = ti.xcom_pull(task_ids="fetch_historical_data")
    if not new_json:
        return

    try:
        new_df = pd.DataFrame(json.loads(new_json))
    except Exception as e:
        return

    if new_df.empty:
        return

    client = _init_minio_client()
    bucket_name = BUCKET_NAME

    now = datetime.now(timezone.utc)
    date_partition = now.strftime("%Y-%m-%d")
    timestamp = now.strftime("%Y%m%dT%H%M%SZ")
    object_name = (
        f"{OBJECT_FOLDER}/date={date_partition}/historical_{timestamp}.json"
    )

    try:
        new_df.drop_duplicates(subset=["coin_id", "datetime"], inplace=True)
        new_df.sort_values(by=["coin_id", "datetime"], inplace=True)

        cleaned_df = _clean_df_for_json(new_df)

        records = cleaned_df.to_dict(orient="records")
        json_bytes = json.dumps(
            records, ensure_ascii=False, indent=2, allow_nan=False
        ).encode("utf-8")

        ensure_bucket(client, bucket_name)

        upload_json_bytes(client, bucket_name, object_name, json_bytes)

    except Exception as e:
        logging.exception("Failed to save historical to MinIO: %s", e)
