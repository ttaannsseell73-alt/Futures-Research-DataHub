"""Public, unauthenticated Binance sources with bounded retries and pagination."""

import csv
import hashlib
import io
import time
import zipfile
from datetime import UTC, datetime
from urllib.parse import quote

import httpx

from .core import INTERVALS, safe_symbol
from .schemas import normalize

ENDPOINTS = {
    "ohlcv": "klines",
    "mark_price": "markPriceKlines",
    "index_price": "indexPriceKlines",
    "funding": "fundingRate",
}


class HTTP:
    def __init__(self, client=None, sleep=time.sleep):
        self.client = client or httpx.Client(timeout=60, follow_redirects=True)
        self.sleep = sleep

    def get(self, url, **kwargs):
        for attempt in range(5):
            try:
                response = self.client.get(url, **kwargs)
            except httpx.TransportError:
                if attempt == 4:
                    raise
                self.sleep(2**attempt)
                continue
            if response.status_code == 418:
                response.raise_for_status()  # Ban: stop immediately.
            if response.status_code == 429 or response.status_code >= 500:
                if attempt < 4:
                    delay = float(response.headers.get("Retry-After", 2**attempt))
                    self.sleep(max(0, min(delay, 300)))
                    continue
            response.raise_for_status()
            return response
        raise RuntimeError("HTTP retry budget exhausted")


class Rest:
    name = "rest"

    def __init__(self, http=None):
        self.http = http or HTTP()

    def fetch(self, kind, symbol, timeframe, start, end):
        rows, cursor = [], start
        endpoint = ENDPOINTS[kind]
        while cursor < end:
            params = {
                "pair" if kind == "index_price" else "symbol": symbol,
                "startTime": cursor,
                "endTime": end - 1,
                "limit": 1000,
            }
            if kind != "funding":
                params["interval"] = timeframe
            page = self.http.get(
                f"https://fapi.binance.com/fapi/v1/{endpoint}", params=params
            ).json()
            if not isinstance(page, list):
                raise ValueError("Unexpected Binance response")
            if not page:
                break
            times = [int(r["fundingTime"] if kind == "funding" else r[0]) for r in page]
            if min(times) < cursor or max(times) >= end or times != sorted(times):
                raise ValueError("REST returned out-of-range or unsorted page")
            rows.extend(page)
            cursor = max(times) + 1
            self.http.sleep(0.25)
        return normalize(rows, kind), {
            "source": "binance_rest",
            "endpoint": endpoint,
            "request_start": start,
            "request_end": end,
        }

    def metadata(self):
        return self.http.get("https://fapi.binance.com/fapi/v1/exchangeInfo").json()


class Vision:
    name = "vision"

    def __init__(self, http=None):
        self.http = http or HTTP()

    def fetch(self, kind, symbol, timeframe, start, end):
        if kind == "funding":
            raise ValueError("Funding uses REST; Vision funding archive is not assumed")
        safe_symbol(symbol)
        if timeframe not in INTERVALS or start % 86_400_000 or end - start != 86_400_000:
            raise ValueError("Vision sync requires a complete UTC day")
        date = datetime.fromtimestamp(start / 1000, UTC).strftime("%Y-%m-%d")
        name = f"{symbol}-{timeframe}-{date}.zip"
        parts = [
            "data",
            "futures",
            "um",
            "daily",
            ENDPOINTS[kind],
            symbol,
            timeframe,
            name,
        ]
        url = "https://data.binance.vision/" + "/".join(quote(part, safe="") for part in parts)
        checksum = self.http.get(url + ".CHECKSUM").text.split()[0].lower()
        raw = self.http.get(url).content
        actual = hashlib.sha256(raw).hexdigest()
        if actual != checksum:
            raise ValueError("Vision archive SHA256 mismatch")
        with zipfile.ZipFile(io.BytesIO(raw)) as z:
            members = [m for m in z.infolist() if m.filename.endswith(".csv")]
            if len(members) != 1 or members[0].file_size > 256 * 1024 * 1024:
                raise ValueError("Unexpected archive content/size")
            rows = list(csv.reader(io.TextIOWrapper(z.open(members[0]), encoding="utf-8-sig")))
        if rows and rows[0][0] in ("open_time", "open time", "open_timestamp"):
            rows = rows[1:]
        return normalize(rows, kind), {
            "source": "binance_vision",
            "url": url,
            "archive_sha256": actual,
        }
