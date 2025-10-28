"""Historical OHLC data extraction helpers."""
from __future__ import annotations

import io
import json
import logging
import time
from typing import Iterable, Optional

import pandas as pd
import requests
from fake_useragent import UserAgent
from minio import Minio
from minio.error import S3Error

from pipelines import config

ua = UserAgent()
API = config.API_HISTORICAL
BUCKET_NAME = config.MINIO_BUCKET_RAW_PRICE
MINIO_ENDPOINT = config.MINIO_ENDPOINT.replace("http://", "").replace("https://", "")
MINIO_ACCESS_KEY = config.MINIO_ACCESS_KEY
MINIO_SECRET_KEY = config.MINIO_SECRET_KEY
MINIO_SECURE = config.MINIO_ENDPOINT.startswith("https")

OBJECT_FOLDER = "historical"
OBJECT_NAME = f"{OBJECT_FOLDER}/historical_data.json"
RATE_LIMIT_SECONDS = 20

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def _init_minio_client() -> Minio:
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )


def _clean_df_for_json(df: pd.DataFrame) -> pd.DataFrame:
    cols = ["coin_id", "datetime", "open", "high", "low", "close", "volume"]
    for col in cols:
        if col not in df.columns:
            df[col] = None
    df = df[cols]

    try:
        df["datetime"] = pd.to_datetime(df["datetime"]).dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        pass

    df = df.astype(object).where(pd.notnull(df), None)
    return df


def _parse_any_json(raw: str) -> list[dict]:
    try:
        data = json.loads(raw)
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            return [data]
    except Exception:
        pass

    decoder = json.JSONDecoder()
    index, length = 0, len(raw)
    output: list[dict] = []
    while index < length:
        while index < length and raw[index].isspace():
            index += 1
        if index >= length:
            break
        obj, idx = decoder.raw_decode(raw, index)
        index = idx
        if isinstance(obj, list):
            output.extend(obj)
        elif isinstance(obj, dict):
            output.append(obj)
    return output


def get_data(coin_data: dict | str, vs_currency: str = "usd", days: int | str = 365) -> Optional[pd.DataFrame]:
    coin_id: Optional[str]
    if isinstance(coin_data, dict):
        coin_id = coin_data.get("nameid") or coin_data.get("id") or coin_data.get("symbol")
    else:
        coin_id = str(coin_data)

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
        df_ohlc = pd.DataFrame(ohlc_data, columns=["timestamp", "open", "high", "low", "close"])

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
        df["datetime"] = pd.to_datetime(df["timestamp"], unit="ms").dt.strftime("%Y-%m-%d %H:%M:%S")
        df["coin_id"] = coin_id
        df = df[["coin_id", "datetime", "open", "high", "low", "close", "volume"]]
        return df
    except requests.exceptions.RequestException as exc:
        logging.error("HTTP error while fetching %s: %s", coin_id, exc)
        return None
    except Exception as exc:  # pragma: no cover - defensive logging
        logging.exception("Unexpected error while fetching %s: %s", coin_id, exc)
        return None


def fetch_historical_data(**context) -> str:
    client = _init_minio_client()
    bucket_name = BUCKET_NAME
    object_name = OBJECT_NAME

    first_run = True
    try:
        client.stat_object(bucket_name, object_name)
        first_run = False
    except S3Error:
        first_run = True

    all_frames: list[pd.DataFrame] = []
    coins_list: Iterable = getattr(config, "COINS", [])
    if not coins_list:
        return "[]"

    for coin in coins_list:
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


def save_historical_to_minio(**context) -> None:
    ti = context.get("ti")
    if not ti:
        return

    new_json = ti.xcom_pull(task_ids="fetch_historical_data")
    if not new_json:
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
            response = client.get_object(bucket_name, object_name)
            raw = response.read()
            if raw:
                parsed = _parse_any_json(raw.decode("utf-8"))
                if parsed:
                    old_df = pd.DataFrame(parsed)
        except S3Error:
            old_df = pd.DataFrame()

        combined_df = pd.concat([old_df, new_df], ignore_index=True)
        combined_df = _clean_df_for_json(combined_df)
        json_data = json.dumps(combined_df.to_dict(orient="records"), ensure_ascii=False).encode("utf-8")

        client.put_object(
            bucket_name,
            object_name,
            data=io.BytesIO(json_data),
            length=len(json_data),
            content_type="application/json",
        )
        logging.info("Saved historical data to Minio object %s", object_name)
    except Exception as exc:  # pragma: no cover - unexpected error handling
        logging.exception("Failed to save historical data: %s", exc)
