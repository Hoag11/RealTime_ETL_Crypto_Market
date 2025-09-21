import sys
import os

parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

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

RATE_LIMIT_SECONDS = 15
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def _init_minio_client():
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )


def get_data(coin_data, vs_currency="usd", days=365):
    # Lấy coin_id (ưu tiên nameid -> id -> symbol)
    if isinstance(coin_data, dict):
        coin_id = (
            coin_data.get("nameid") or coin_data.get("id") or coin_data.get("symbol")
        )
    else:
        coin_id = coin_data

    if not coin_id:
        logging.warning("No coin_id found in coin_data: %s", coin_data)
        return None

    url = f"{API}/coins/{coin_id}/ohlc"
    params = {"vs_currency": vs_currency, "days": int(days)}

    try:
        resp = requests.get(
            url, params=params, headers={"User-Agent": ua.random}, timeout=60
        )
        resp.raise_for_status()
        data = resp.json()
        if not data:
            logging.info("No OHLC data for %s (days=%s)", coin_id, days)
            return None

        df = pd.DataFrame(data, columns=["timestamp", "open", "high", "low", "close"])
        # convert timestamp (ms) -> string (consistent for dedupe)
        df["datetime"] = pd.to_datetime(df["timestamp"], unit="ms").dt.strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        df["coin_id"] = coin_id
        df = df[["coin_id", "datetime", "open", "high", "low", "close"]]
        logging.info("Fetched %d rows for %s (days=%s)", len(df), coin_id, days)
        return df

    except requests.exceptions.RequestException as e:
        logging.error("HTTP error while fetching %s: %s", coin_id, e)
        return None
    except Exception as e:
        logging.exception("Unexpected error while fetching %s: %s", coin_id, e)
        return None


# ---------- AIRFLOW TASKS (keeps signatures for DAG) ----------
def fetch_historical_data(**context):
    # init minio client to check existence
    client = _init_minio_client()
    bucket_name = BUCKET_NAME
    object_name = OBJECT_NAME

    # check first run
    first_run = True
    try:
        client.stat_object(bucket_name, object_name)
        first_run = False
        logging.info("Found existing historical object -> incremental mode")
    except S3Error:
        # object not found -> first run
        first_run = True
        logging.info(
            "No existing historical object -> first run (fetching extended data)"
        )
    except Exception as e:
        # network or other error: assume incremental to be safe
        logging.warning(
            "Error checking existing object: %s. Proceeding as first_run=%s",
            e,
            first_run,
        )

    all_frames = []
    coins_list = getattr(configs, "COINS", [])
    if not coins_list:
        logging.warning("configs.COINS is empty. Nothing to fetch.")
        return "[]"

    for idx, coin in enumerate(coins_list, start=1):
        days = 365 if first_run else 1
        try:
            df = get_data(coin, vs_currency="usd", days=days)
            if df is not None and not df.empty:
                all_frames.append(df)
        except Exception as e:
            logging.exception("Error processing coin %s: %s", coin, e)

        # rate limit sleep
        time.sleep(RATE_LIMIT_SECONDS)

        if idx % 100 == 0:
            logging.info("Processed %d/%d coins", idx, len(coins_list))

    if not all_frames:
        logging.warning("No data fetched in this run.")
        return "[]"

    result_df = pd.concat(all_frames, ignore_index=True)
    # normalize dtypes to make JSON stable
    result_df = result_df.astype({"coin_id": str, "datetime": str})
    json_str = result_df.to_json(orient="records", force_ascii=False)
    logging.info(
        "Fetched total %d records, returning JSON string for XCom", len(result_df)
    )
    return json_str


def save_historical_to_minio(**context):
    ti = context.get("ti")
    if not ti:
        logging.error(
            "No task instance (ti) in context. This function is meant to run as Airflow task."
        )
        return

    # Pull from XCom - task_id must match your DAG task id for fetch_historical_data
    new_json = ti.xcom_pull(task_ids="fetch_historical_data")
    if not new_json:
        logging.warning("No new historical JSON found in XCom.")
        return

    try:
        new_df = pd.DataFrame(json.loads(new_json))
    except Exception as e:
        logging.error("Failed to parse JSON from XCom: %s", e)
        return

    if new_df.empty:
        logging.warning("New DataFrame is empty. Nothing to save.")
        return

    # ensure columns exist
    for c in ["coin_id", "datetime", "open", "high", "low", "close"]:
        if c not in new_df.columns:
            new_df[c] = None

    client = _init_minio_client()
    bucket_name = BUCKET_NAME
    object_name = OBJECT_NAME

    # read old data if exists
    try:
        old_df = pd.DataFrame()
        try:
            resp = client.get_object(bucket_name, object_name)
            old_raw = resp.read().decode("utf-8")
            try:
                old_df = (
                    pd.DataFrame(json.loads(old_raw))
                    if old_raw.strip()
                    else pd.DataFrame()
                )
            except Exception as e:
                logging.warning("Existing object is not valid JSON or empty: %s", e)
                old_df = pd.DataFrame()
            finally:
                # close the object stream
                try:
                    resp.close()
                    resp.release_conn()
                except Exception:
                    pass
        except S3Error:
            # object does not exist => first time
            old_df = pd.DataFrame()
        except Exception as e:
            logging.warning("Error fetching existing object: %s", e)
            old_df = pd.DataFrame()

        # combine
        if not old_df.empty:
            combined_df = pd.concat([old_df, new_df], ignore_index=True)
        else:
            combined_df = new_df.copy()

        # ensure key columns exist before dedupe
        for c in ["coin_id", "datetime"]:
            if c not in combined_df.columns:
                combined_df[c] = combined_df[c].astype(str)

        # dedupe & sort
        combined_df.drop_duplicates(subset=["coin_id", "datetime"], inplace=True)
        combined_df.sort_values(by=["coin_id", "datetime"], inplace=True)

        # convert to JSON bytes
        records = combined_df.to_dict(orient="records")
        json_bytes = json.dumps(records, ensure_ascii=False, indent=2).encode("utf-8")

        # ensure bucket exists
        if not client.bucket_exists(bucket_name):
            logging.info("Bucket %s does not exist. Creating...", bucket_name)
            client.make_bucket(bucket_name)

        # upload
        client.put_object(
            bucket_name,
            object_name,
            data=io.BytesIO(json_bytes),
            length=len(json_bytes),
            content_type="application/json",
        )

        logging.info(
            "Saved %d records to %s/%s", len(combined_df), bucket_name, object_name
        )

    except Exception as e:
        logging.exception("Failed to save historical to MinIO: %s", e)
        return
