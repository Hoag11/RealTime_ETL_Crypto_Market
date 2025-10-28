import io
import json
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, Iterable, Iterator, List, Optional

import pandas as pd
from minio import Minio
from minio.error import S3Error
from requests import RequestException

from configs import configs
from scripts.common import http_client
API = getattr(configs, "API_HISTORICAL", "https://api.coingecko.com/api/v3")
BUCKET_NAME = getattr(configs, "MINIO_BUCKET_RAW_PRICE", None)
MINIO_ENDPOINT = configs.MINIO_ENDPOINT.replace("http://", "")
MINIO_ACCESS_KEY = configs.MINIO_ACCESS_KEY
MINIO_SECRET_KEY = configs.MINIO_SECRET_KEY
MINIO_SECURE = False

OBJECT_FOLDER = "historical"
OBJECT_NAME = f"{OBJECT_FOLDER}/historical_data.json"

RATE_LIMIT_SECONDS = getattr(configs, "HISTORICAL_RATE_LIMIT_SECONDS", 20.0)
BATCH_SIZE = max(1, getattr(configs, "HISTORICAL_BATCH_SIZE", 10))
TOP_N = getattr(configs, "HISTORICAL_TOP_N", None)
STALE_THRESHOLD_HOURS = getattr(configs, "HISTORICAL_STALENESS_HOURS", 6.0)
MIN_SLEEP_SECONDS = getattr(configs, "HISTORICAL_MIN_SLEEP", 1.0)
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


def _chunked(items: List, size: int) -> Iterator[List]:
    for idx in range(0, len(items), size):
        yield items[idx : idx + size]


def _extract_coin_id(coin_data) -> Optional[str]:
    if isinstance(coin_data, dict):
        return (
            coin_data.get("nameid")
            or coin_data.get("id")
            or coin_data.get("symbol")
        )
    if coin_data:
        return str(coin_data)
    return None


def _build_last_seen(records: Iterable[dict]) -> Dict[str, datetime]:
    last_seen: Dict[str, datetime] = {}
    for record in records:
        coin_id = record.get("coin_id") or record.get("id")
        dt_str = record.get("datetime")
        if not coin_id or not dt_str:
            continue
        try:
            dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
        current = last_seen.get(coin_id)
        if current is None or dt > current:
            last_seen[coin_id] = dt
    return last_seen


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
        resp_ohlc = http_client.get(
            url_ohlc,
            params={"vs_currency": vs_currency, "days": int(days)},
            timeout=60,
        )
        ohlc_data = resp_ohlc.json()
        if not ohlc_data:
            logging.info("No OHLC data for %s (days=%s)", coin_id, days)
            return None
        df_ohlc = pd.DataFrame(
            ohlc_data, columns=["timestamp", "open", "high", "low", "close"]
        )

        url_mc = f"{API}/coins/{coin_id}/market_chart"
        resp_mc = http_client.get(
            url_mc,
            params={"vs_currency": vs_currency, "days": int(days)},
            timeout=60,
        )
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

    except RequestException as e:
        logging.error("HTTP error while fetching %s: %s", coin_id, e)
        return None
    except Exception as e:
        logging.exception("Unexpected error while fetching %s: %s", coin_id, e)
        return None


def fetch_historical_data(**context):
    client = _init_minio_client()
    bucket_name = BUCKET_NAME
    object_name = OBJECT_NAME

    first_run = True
    existing_records: List[dict] = []
    response = None
    try:
        client.stat_object(bucket_name, object_name)
        first_run = False
        try:
            response = client.get_object(bucket_name, object_name)
            raw = response.read().decode("utf-8")
            existing_records = _parse_any_json(raw)
        finally:
            try:
                if response is not None:
                    response.close()
                    response.release_conn()
            except Exception:
                pass
    except S3Error:
        first_run = True
    except Exception as exc:
        logging.warning("Unable to load existing historical cache: %s", exc)

    last_seen: Dict[str, datetime] = {}
    if not first_run and existing_records:
        last_seen = _build_last_seen(existing_records)

    coins_list = list(getattr(configs, "COINS", []))
    if TOP_N:
        coins_list = coins_list[: int(TOP_N)]

    if not coins_list:
        logging.warning("No coins configured for historical job")
        return "[]"

    if not first_run and STALE_THRESHOLD_HOURS > 0:
        threshold = datetime.utcnow() - timedelta(hours=STALE_THRESHOLD_HOURS)
        filtered_coins = []
        for coin in coins_list:
            coin_id = _extract_coin_id(coin)
            if not coin_id:
                logging.debug("Skipping coin without identifier: %s", coin)
                continue
            last_time = last_seen.get(coin_id)
            if last_time is None or last_time < threshold:
                filtered_coins.append(coin)
            else:
                logging.info(
                    "Skipping %s — last update %s is within %.1f hours",
                    coin_id,
                    last_time,
                    STALE_THRESHOLD_HOURS,
                )
        coins_list = filtered_coins

    if not coins_list:
        logging.info("Historical data up to date; nothing to fetch")
        return "[]"

    total_batches = (len(coins_list) + BATCH_SIZE - 1) // BATCH_SIZE
    all_frames = []

    for batch_index, batch in enumerate(_chunked(coins_list, BATCH_SIZE), start=1):
        logging.info(
            "Processing historical batch %s/%s (%s coins)",
            batch_index,
            total_batches,
            len(batch),
        )
        batch_start = time.monotonic()
        for coin in batch:
            coin_id = _extract_coin_id(coin)
            days = 365 if first_run else 1
            logging.info("Fetching historical data for %s (days=%s)", coin_id, days)
            df = get_data(coin, vs_currency="usd", days=days)
            if df is not None and not df.empty:
                all_frames.append(df)
        batch_duration = time.monotonic() - batch_start

        if batch_index < total_batches:
            sleep_for = max(0.0, RATE_LIMIT_SECONDS - batch_duration)
            if 0 < sleep_for < MIN_SLEEP_SECONDS:
                sleep_for = MIN_SLEEP_SECONDS
            if sleep_for > 0:
                logging.info(
                    "Sleeping %.2fs before next historical batch (batch time %.2fs)",
                    sleep_for,
                    batch_duration,
                )
                time.sleep(sleep_for)

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
        logging.exception("Failed to save historical to MinIO: %s", e)
