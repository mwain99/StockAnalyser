"""
polygon_client.py
-----------------
Thin, retry-aware HTTP client for the Polygon.io REST API.

Rate limiting
~~~~~~~~~~~~~
Polygon free tier: 5 requests / minute.
A minimum gap of 13 seconds between requests is enforced so we never hit 429 in normal
operation. If a 429 does come back anyway we back off and retry automatically.
"""
from __future__ import annotations

import logging
import time
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from stockanalyser.config.settings import Settings, settings as default_settings

logger = logging.getLogger(__name__)

FREE_TIER_MIN_INTERVAL: float = 13.0


class PolygonClient:
    AGGS_ENDPOINT = (
        "{base_url}/v2/aggs/ticker/{ticker}/range/1/day/{start}/{end}"
        "?adjusted=true&sort=asc&limit=50000"
    )

    def __init__(self, cfg: Settings = default_settings) -> None:
        self._cfg = cfg
        self._session = self._build_session()
        self._last_request_time: float = 0.0

    def get_daily_aggregates(
        self,
        ticker: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> list[dict[str, Any]]:
        start = start_date or self._cfg.start_date
        end = end_date or self._cfg.end_date
        url = self.AGGS_ENDPOINT.format(
            base_url=self._cfg.api_base_url,
            ticker=ticker.upper(),
            start=start,
            end=end,
        )

        self._throttle()
        logger.info("Fetching %s  [%s → %s]", ticker, start, end)
        response = self._get_with_retry(url)
        payload = response.json()

        status = payload.get("status", "")
        if status not in ("OK", "DELAYED"):
            logger.warning(
                "Unexpected status '%s' for %s: %s",
                status,
                ticker,
                payload.get("error", ""),
            )
            return []

        results = payload.get("results") or []
        logger.debug("  → %d bars received for %s", len(results), ticker)
        return results

    def _throttle(self) -> None:
        elapsed = time.monotonic() - self._last_request_time
        wait = FREE_TIER_MIN_INTERVAL - elapsed
        if wait > 0:
            logger.debug("Rate-limit throttle: sleeping %.1fs", wait)
            time.sleep(wait)
        self._last_request_time = time.monotonic()

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        session.headers.update(self._cfg.api_headers)

        retry_strategy = Retry(
            total=self._cfg.api_max_retries,
            backoff_factor=self._cfg.api_retry_backoff,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["GET"],
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def _get_with_retry(self, url: str) -> requests.Response:
        """
        Perform a GET, handling 429 explicitly with a 60-second back-off
        on top of the throttle already applied.
        """
        for attempt in range(self._cfg.api_max_retries + 1):
            resp = self._session.get(url, timeout=self._cfg.api_request_timeout)
            if resp.status_code == 429:
                wait = 60.0 * (attempt + 1)
                logger.warning(
                    "Rate-limited (429) on attempt %d — waiting %.0fs before retry",
                    attempt + 1,
                    wait,
                )
                time.sleep(wait)
                self._last_request_time = time.monotonic()
                continue
            resp.raise_for_status()
            return resp

        raise RuntimeError(
            f"Exceeded retry budget for {url} due to persistent rate limiting."
        )