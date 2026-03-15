"""
ticker_validator.py
-------------------
Validates ticker symbols against the Polygon reference API before
ingestion begins.

Catches problems like:
  - Stale tickers (e.g. FB → META, ANTM → ELV)
  - Delisted or inactive symbols
  - Company name mismatch (ticker is valid but belongs to a different company)
  - Duplicate symbols in the input file
  - Malformed symbols (empty, numeric, too long)

Design
~~~~~~
Validation runs before any bar data is fetched so the user gets a clear
report of all bad tickers upfront rather than discovering them after a
20-minute fetch run.

The name-match check is the key defence against stale tickers that have
been reused by a different company (e.g. FB was Meta, now belongs to a
small fintech). We use fuzzy keyword matching rather than exact string
comparison to handle minor naming differences like "Inc." vs "Incorporated".
"""
from __future__ import annotations

import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field

import requests

from stockanalyser.config.settings import Settings, settings as default_settings

logger = logging.getLogger(__name__)

_SYMBOL_RE = re.compile(r"^[A-Z]{1,5}(\.[A-Z]{1,2})?$")

# Common suffixes to strip before comparing names
_STRIP_WORDS = re.compile(
    r"\b(inc|incorporated|corp|corporation|co|company|ltd|limited|plc|llc"
    r"|group|holdings|international|class\s+[a-c]|the)\b",
    re.IGNORECASE,
)


def _normalise_name(name: str) -> set[str]:
    """
    Strip punctuation, common suffixes, and return a set of meaningful
    words for fuzzy comparison.
    """
    name = name.lower()
    name = re.sub(r"[^a-z0-9\s]", " ", name)
    name = _STRIP_WORDS.sub(" ", name)
    return {w for w in name.split() if len(w) > 2}


def _names_match(expected: str, actual: str) -> bool:
    """
    Return True if the expected and actual company names share enough
    meaningful words to be considered the same company.
    Requires at least one meaningful word in common.

    If the expected name has meaningful words but the actual name reduces
    to nothing (e.g. just "FB" or "FB Inc"), we treat that as a mismatch
    rather than giving benefit of the doubt — a major company like Meta
    would never have a one/two-letter name on Polygon.
    """
    expected_words = _normalise_name(expected)
    actual_words = _normalise_name(actual)

    # Can't compare if expected has no meaningful words
    if not expected_words:
        return True

    # Actual name reduced to nothing but expected has substance — mismatch
    if not actual_words:
        return False

    overlap = expected_words & actual_words
    return len(overlap) > 0


@dataclass
class TickerValidationResult:
    valid: list[str] = field(default_factory=list)
    invalid: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    details: dict[str, dict] = field(default_factory=dict)

    @property
    def has_errors(self) -> bool:
        return len(self.invalid) > 0

    def report(self) -> str:
        lines = [
            f"\n{'='*60}",
            f" TICKER VALIDATION REPORT",
            f"{'='*60}",
            f" Total tickers   : {len(self.valid) + len(self.invalid)}",
            f" Valid           : {len(self.valid)}",
            f" Invalid/Unknown : {len(self.invalid)}",
            f" Warnings        : {len(self.warnings)}",
        ]

        if self.warnings:
            lines.append(f"\n WARNINGS:")
            for w in self.warnings:
                lines.append(f"   ⚠  {w}")

        if self.invalid:
            lines.append(f"\n INVALID TICKERS (will be skipped):")
            for sym in self.invalid:
                detail = self.details.get(sym, {})
                reason = detail.get("reason", "Unknown")
                lines.append(f"   ✗  {sym:<10} {reason}")

        lines.append(f"{'='*60}\n")
        return "\n".join(lines)


class TickerValidator:
    """
    Validates a list of (company_name, symbol) pairs using the Polygon
    reference API.
    """

    REFERENCE_ENDPOINT = "{base_url}/v3/reference/tickers/{ticker}"

    def __init__(self, cfg: Settings = default_settings) -> None:
        self._cfg = cfg
        self._session = requests.Session()
        self._session.headers.update(cfg.api_headers)

    def validate(
        self,
        symbols: list[str],
        company_names: dict[str, str] | None = None,
    ) -> TickerValidationResult:
        """
        Validate all symbols and return a TickerValidationResult.

        Parameters
        ----------
        symbols:
            List of ticker symbols to validate.
        company_names:
            Optional dict mapping symbol → expected company name.
            When provided, the name returned by Polygon is compared
            against the expected name to catch reused tickers.
        """
        result = TickerValidationResult()
        company_names = company_names or {}

        seen: set[str] = set()
        to_check_api: list[str] = []

        # ---- 1. Format checks ------------------------------------------ #
        for sym in symbols:
            sym = sym.strip().upper()

            if not sym:
                result.invalid.append(sym)
                result.details[sym] = {"reason": "Empty symbol"}
                continue

            if sym in seen:
                result.warnings.append(f"Duplicate symbol: {sym}")
                continue
            seen.add(sym)

            if not _SYMBOL_RE.match(sym):
                result.invalid.append(sym)
                result.details[sym] = {
                    "reason": f"Invalid format '{sym}' — expected 1-5 uppercase letters"
                }
                continue

            to_check_api.append(sym)

        # ---- 2. API validation ------------------------------------------ #
        logger.info(
            "Validating %d ticker symbols against Polygon reference API...",
            len(to_check_api),
        )

        api_results = self._check_api_concurrent(to_check_api, company_names)

        for sym, detail in api_results.items():
            result.details[sym] = detail
            if detail.get("valid"):
                result.valid.append(sym)
                if detail.get("warning"):
                    result.warnings.append(f"{sym}: {detail['warning']}")
            else:
                result.invalid.append(sym)

        return result

    def _check_api_concurrent(
        self,
        symbols: list[str],
        company_names: dict[str, str],
    ) -> dict[str, dict]:
        results: dict[str, dict] = {}

        with ThreadPoolExecutor(max_workers=self._cfg.api_max_workers) as executor:
            future_to_sym = {
                executor.submit(
                    self._check_one, sym, company_names.get(sym, "")
                ): sym
                for sym in symbols
            }
            for future in as_completed(future_to_sym):
                sym = future_to_sym[future]
                try:
                    results[sym] = future.result()
                except Exception as exc:
                    logger.warning("Could not validate %s: %s", sym, exc)
                    results[sym] = {
                        "valid": True,
                        "warning": f"Could not verify via API: {exc}",
                    }

        return results

    def _check_one(self, symbol: str, expected_name: str = "") -> dict:
        """Check a single ticker, optionally comparing the company name."""
        url = self.REFERENCE_ENDPOINT.format(
            base_url=self._cfg.api_base_url,
            ticker=symbol,
        )

        try:
            resp = self._session.get(url, timeout=self._cfg.api_request_timeout)
        except requests.RequestException as exc:
            return {"valid": True, "warning": f"API unreachable: {exc}"}

        if resp.status_code == 404:
            return {
                "valid": False,
                "reason": "Ticker not found on Polygon — may be delisted or renamed",
            }

        if resp.status_code == 403:
            return {"valid": True, "warning": "Could not verify (API permission issue)"}

        if not resp.ok:
            return {"valid": True, "warning": f"API returned {resp.status_code}"}

        data = resp.json().get("results", {})
        if not data:
            return {"valid": False, "reason": "No reference data returned by Polygon"}

        active = data.get("active", True)
        actual_name = data.get("name", "")
        ticker_type = data.get("type", "")

        if not active:
            return {
                "valid": False,
                "reason": f"Ticker is inactive/delisted (was: '{actual_name}')",
            }

        # ---- Type check — must be a common stock ----------------------- #
        # Polygon uses "CS" for common stocks. Anything else means the
        # ticker is not a stock and should be rejected.
        if ticker_type and ticker_type.upper() != "CS":
            return {
                "valid": False,
                "reason": (
                    f"'{actual_name}' is not a stock (Polygon type: '{ticker_type}')"
                ),
            }

        # ---- Name mismatch check ---------------------------------------- #
        if expected_name and actual_name:
            if not _names_match(expected_name, actual_name):
                return {
                    "valid": False,
                    "reason": (
                        f"Company name mismatch — "
                        f"expected '{expected_name}' "
                        f"but Polygon returned '{actual_name}'. "
                        f"This ticker may have been reused by a different company."
                    ),
                }

        return {"valid": True, "name": actual_name}