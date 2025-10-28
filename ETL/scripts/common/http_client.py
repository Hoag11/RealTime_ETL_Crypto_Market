"""HTTP client wrapper shared across batch and streaming jobs."""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

import requests
from fake_useragent import UserAgent
from requests import Response
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


@dataclass
class HttpClientConfig:
    """Configuration for the shared HTTP client."""

    timeout: float = 30.0
    max_retries: int = 3
    backoff_factor: float = 0.5
    status_forcelist: tuple[int, ...] = (429, 500, 502, 503, 504)
    allowed_methods: Optional[frozenset[str]] = frozenset(
        {"GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"}
    )


class HttpClient:
    """Wrapper around :class:`requests.Session` with retry, timeout and logging."""

    def __init__(self, config: Optional[HttpClientConfig] = None) -> None:
        self.config = config or HttpClientConfig()
        self._session = requests.Session()
        self._ua = UserAgent()
        self._ua_lock = threading.Lock()
        self._configure_session()

    def _configure_session(self) -> None:
        retries = Retry(
            total=self.config.max_retries,
            connect=self.config.max_retries,
            read=self.config.max_retries,
            backoff_factor=self.config.backoff_factor,
            status_forcelist=self.config.status_forcelist,
            allowed_methods=self.config.allowed_methods,
            raise_on_status=False,
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(max_retries=retries)
        self._session.mount("http://", adapter)
        self._session.mount("https://", adapter)

    def _get_user_agent(self) -> str:
        with self._ua_lock:
            try:
                return self._ua.random
            except Exception:
                # Fallback to a generic UA if the random provider fails
                return "Mozilla/5.0 (compatible; RealTimeETLBot/1.0)"

    def request(
        self,
        method: str,
        url: str,
        *,
        timeout: Optional[float] = None,
        headers: Optional[Dict[str, str]] = None,
        **kwargs: Any,
    ) -> Response:
        """Perform an HTTP request with retries and standardised logging."""

        merged_headers: Dict[str, str] = {
            "User-Agent": self._get_user_agent(),
            "Accept": "application/json, text/plain, */*",
        }
        if headers:
            merged_headers.update(headers)

        request_timeout = timeout or self.config.timeout
        start_time = time.monotonic()
        try:
            response = self._session.request(
                method.upper(),
                url,
                timeout=request_timeout,
                headers=merged_headers,
                **kwargs,
            )
            duration = time.monotonic() - start_time
            logger.info(
                "HTTP %s %s -> %s (%.2fs)",
                method.upper(),
                url,
                response.status_code,
                duration,
            )
            response.raise_for_status()
            return response
        except requests.RequestException as exc:
            duration = time.monotonic() - start_time
            logger.warning(
                "HTTP %s %s failed after %.2fs: %s",
                method.upper(),
                url,
                duration,
                exc,
            )
            raise

    def get(
        self,
        url: str,
        *,
        timeout: Optional[float] = None,
        headers: Optional[Dict[str, str]] = None,
        **kwargs: Any,
    ) -> Response:
        return self.request("GET", url, timeout=timeout, headers=headers, **kwargs)

    def close(self) -> None:
        self._session.close()


http_client = HttpClient()
