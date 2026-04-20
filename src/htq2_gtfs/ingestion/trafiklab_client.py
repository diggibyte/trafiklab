"""HTTP client for Trafiklab.se GTFS APIs.

Handles both Realtime (protobuf) and Static (ZIP) downloads with
retry logic and exponential backoff.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Optional

import requests

from htq2_gtfs.config import (
    HTTP_TIMEOUT_SECONDS,
    MAX_RETRIES,
    REALTIME_FEEDS,
    REGION,
    RETRY_BACKOFF_SECONDS,
    STATIC_FILES,
    TRAFIKLAB_REALTIME_BASE,
    TRAFIKLAB_STATIC_BASE,
)
from htq2_gtfs.helpers.logging_config import get_logger

logger = get_logger(__name__)


@dataclass
class DownloadResult:
    """Result of a single file download attempt."""
    url: str
    filename: str
    success: bool
    size_bytes: int = 0
    error: Optional[str] = None
    attempts: int = 0


class TrafiklabClient:
    """HTTP client for the Trafiklab.se GTFS Regional API.

    Args:
        realtime_api_key: API key for GTFS Realtime feeds.
        static_api_key: API key for GTFS Static downloads.
        region: Region identifier (default: 'skane').
    """

    def __init__(
        self,
        realtime_api_key: str,
        static_api_key: str,
        region: str = REGION,
    ) -> None:
        self.realtime_api_key = realtime_api_key
        self.static_api_key = static_api_key
        self.region = region
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "HTQ2-GTFS-Pipeline/1.0 (Databricks)",
        })

    def fetch_realtime_feed(self, feed_name: str) -> bytes:
        """Download a single realtime protobuf feed.

        Args:
            feed_name: One of 'ServiceAlerts.pb', 'TripUpdates.pb',
                       'VehiclePositions.pb'.

        Returns:
            Raw protobuf bytes.

        Raises:
            requests.HTTPError: If all retries are exhausted.
        """
        base_url = TRAFIKLAB_REALTIME_BASE.format(region=self.region)
        url = f"{base_url}/{feed_name}?key={self.realtime_api_key}"
        return self._download_with_retry(url, feed_name)

    def fetch_static_file(self, file_template: str) -> bytes:
        """Download a static GTFS ZIP file.

        Args:
            file_template: Filename template with {region} placeholder,
                          e.g. '{region}.zip' or '{region}_extra.zip'.

        Returns:
            Raw ZIP bytes.
        """
        filename = file_template.format(region=self.region)
        base_url = TRAFIKLAB_STATIC_BASE.format(region=self.region)
        url = f"{base_url}/{filename}?key={self.static_api_key}"
        return self._download_with_retry(url, filename)

    def fetch_all_realtime(self) -> dict[str, bytes]:
        """Download all 3 realtime feeds.

        Returns:
            Dict mapping feed name to raw bytes.
            Only includes successfully downloaded feeds.
        """
        results: dict[str, bytes] = {}
        for feed in REALTIME_FEEDS:
            try:
                data = self.fetch_realtime_feed(feed)
                results[feed] = data
                logger.info(
                    f"Downloaded {feed}",
                    extra={"file_count": 1, "status": "success"},
                )
            except requests.HTTPError as e:
                logger.error(
                    f"Failed to download {feed}: {e}",
                    extra={"status": "failed", "error": str(e)},
                )
        return results

    def fetch_all_static(self) -> dict[str, bytes]:
        """Download all static GTFS files.

        Returns:
            Dict mapping filename to raw bytes.
        """
        results: dict[str, bytes] = {}
        for template in STATIC_FILES:
            filename = template.format(region=self.region)
            try:
                data = self.fetch_static_file(template)
                results[filename] = data
                logger.info(
                    f"Downloaded static file: {filename} ({len(data):,} bytes)",
                    extra={"status": "success"},
                )
            except requests.HTTPError as e:
                logger.error(
                    f"Failed to download {filename}: {e}",
                    extra={"status": "failed", "error": str(e)},
                )
        return results

    def _download_with_retry(self, url: str, label: str) -> bytes:
        """Download URL with exponential backoff retry.

        Args:
            url: Full URL including API key.
            label: Human-readable name for logging.

        Returns:
            Response body as bytes.

        Raises:
            requests.HTTPError: After all retries exhausted.
        """
        last_error: Optional[Exception] = None

        for attempt in range(MAX_RETRIES):
            try:
                response = self._session.get(
                    url,
                    timeout=HTTP_TIMEOUT_SECONDS,
                    stream=False,
                )
                response.raise_for_status()
                return response.content

            except (requests.HTTPError, requests.ConnectionError,
                    requests.Timeout) as e:
                last_error = e
                if attempt < MAX_RETRIES - 1:
                    backoff = RETRY_BACKOFF_SECONDS[min(attempt, len(RETRY_BACKOFF_SECONDS) - 1)]
                    logger.warning(
                        f"Attempt {attempt + 1}/{MAX_RETRIES} failed for {label}: {e}. "
                        f"Retrying in {backoff}s..."
                    )
                    time.sleep(backoff)
                else:
                    logger.error(
                        f"All {MAX_RETRIES} attempts failed for {label}: {e}"
                    )

        raise last_error  # type: ignore[misc]

    def close(self) -> None:
        """Close the underlying HTTP session."""
        self._session.close()

    def __enter__(self) -> TrafiklabClient:
        return self

    def __exit__(self, *args) -> None:
        self.close()
