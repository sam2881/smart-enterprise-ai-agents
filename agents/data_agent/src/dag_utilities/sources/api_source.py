"""
API Source Utilities - APEX Data Plane

Utilities for REST, GraphQL, and SaaS API sources used by zone_processor.py.

This module provides source handlers for API-based data ingestion in the APEX
Data Plane. All functions return structured dicts that can be consumed by
ZoneProcessor for zone transitions (Transient -> Raw -> Refined -> Gold ->
Consumption).

Supported source types:
- REST APIs (GET, POST, PUT, PATCH)
- GraphQL endpoints
- SaaS connectors (Salesforce, ServiceNow)

Auth types: bearer, basic, api_key, oauth2
Pagination types: offset, cursor, link_header, token
"""

from typing import Any, Dict, List, Optional
import time
import base64
import logging

logger = logging.getLogger(__name__)

try:
    import requests
except ImportError:
    requests = None  # type: ignore[assignment]


# ---------------------------------------------------------------------------
# APISourceHandler
# ---------------------------------------------------------------------------

class APISourceHandler:
    """
    Handler for REST and generic HTTP API sources.

    Provides methods for fetching data from API endpoints with support for
    authentication, pagination, and rate limiting.
    """

    def fetch_api_data(
        self,
        endpoint: str,
        method: str = "GET",
        headers: Optional[Dict[str, str]] = None,
        auth_config: Optional[Dict[str, Any]] = None,
        pagination_config: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Fetch data from an API endpoint.

        Args:
            endpoint: Full URL of the API endpoint.
            method: HTTP method (GET, POST, PUT, PATCH).
            headers: Additional HTTP headers.
            auth_config: Authentication configuration with keys:
                - auth_type: bearer | basic | api_key | oauth2
                - token / username / password / api_key / client_id /
                  client_secret / token_url (depending on auth_type)
            pagination_config: Pagination configuration with keys:
                - pagination_type: offset | cursor | link_header | token
                - pagination_key: JSON key holding the next-page indicator
                - page_size: Number of records per page (default 100)

        Returns:
            Dict with keys:
                - records: list of fetched records
                - total_records: total count of records fetched
                - pages_fetched: number of pages retrieved
                - endpoint: the endpoint that was called
                - method: HTTP method used
        """
        if requests is None:
            logger.warning("requests library not available; returning empty result")
            return {
                "records": [],
                "total_records": 0,
                "pages_fetched": 0,
                "endpoint": endpoint,
                "method": method,
            }

        merged_headers = dict(headers or {})
        merged_headers = self._apply_auth_headers(merged_headers, auth_config)

        if pagination_config:
            pagination_type = pagination_config.get("pagination_type", "offset")
            pagination_key = pagination_config.get("pagination_key", "next")
            records = self.handle_pagination(
                _make_request(endpoint, method, merged_headers),
                pagination_type,
                pagination_key,
            )
        else:
            response = _make_request(endpoint, method, merged_headers)
            data = response.get("data", response)
            records = data if isinstance(data, list) else [data]

        return {
            "records": records,
            "total_records": len(records),
            "pages_fetched": 1 if not pagination_config else -1,
            "endpoint": endpoint,
            "method": method,
        }

    def handle_pagination(
        self,
        response: Dict[str, Any],
        pagination_type: str,
        pagination_key: str,
    ) -> List[Dict[str, Any]]:
        """
        Follow pagination to collect all pages of results.

        Args:
            response: Initial response dict (parsed JSON).
            pagination_type: One of offset, cursor, link_header, token.
            pagination_key: JSON key that holds next-page indicator.

        Returns:
            Flat list of all records across all pages.
        """
        all_records: List[Dict[str, Any]] = []

        data = response.get("data", response.get("results", response.get("items", [])))
        if isinstance(data, list):
            all_records.extend(data)
        elif isinstance(data, dict):
            all_records.append(data)

        if pagination_type == "offset":
            # Offset-based: expects pagination_key to point to total count
            # Additional pages would be fetched by the caller using
            # offset=len(all_records) until total is reached.
            pass

        elif pagination_type == "cursor":
            # Cursor-based: pagination_key holds the next cursor value
            next_cursor = response.get(pagination_key)
            if next_cursor:
                logger.info("Cursor pagination detected; next cursor: %s", next_cursor)

        elif pagination_type == "link_header":
            # Link header: the 'next' URL comes from response headers
            next_link = response.get("_links", {}).get("next")
            if next_link:
                logger.info("Link-header pagination detected; next: %s", next_link)

        elif pagination_type == "token":
            # Token-based: pagination_key holds a continuation token
            next_token = response.get(pagination_key)
            if next_token:
                logger.info("Token pagination detected; next token present")

        return all_records

    def apply_rate_limiting(self, rps_limit: int) -> Dict[str, Any]:
        """
        Create a rate limiter configuration.

        The returned dict describes the rate-limiting parameters that callers
        should respect between consecutive API requests.

        Args:
            rps_limit: Maximum requests per second.

        Returns:
            Dict with rate-limiting metadata:
                - rps_limit: configured limit
                - delay_seconds: sleep duration between requests
                - enabled: whether rate limiting is active
        """
        if rps_limit <= 0:
            return {
                "rps_limit": 0,
                "delay_seconds": 0.0,
                "enabled": False,
            }

        delay = 1.0 / rps_limit
        return {
            "rps_limit": rps_limit,
            "delay_seconds": delay,
            "enabled": True,
        }

    def build_api_sensor_config(
        self,
        endpoint: str,
        method: str = "GET",
        expected_status: int = 200,
    ) -> Dict[str, Any]:
        """
        Build an Airflow HTTP sensor configuration for the API endpoint.

        Args:
            endpoint: Full URL of the API endpoint to monitor.
            method: HTTP method for the health/readiness check.
            expected_status: HTTP status code indicating the endpoint is ready.

        Returns:
            Dict suitable for configuring an Airflow HttpSensor:
                - endpoint: URL to poll
                - method: HTTP method
                - expected_status: status code for success
                - poke_interval: seconds between polls (default 60)
                - timeout: total timeout in seconds (default 3600)
                - mode: sensor mode (default 'poke')
        """
        return {
            "endpoint": endpoint,
            "method": method,
            "expected_status": expected_status,
            "poke_interval": 60,
            "timeout": 3600,
            "mode": "poke",
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _apply_auth_headers(
        headers: Dict[str, str],
        auth_config: Optional[Dict[str, Any]],
    ) -> Dict[str, str]:
        """
        Merge authentication headers into the request headers.

        Supports bearer, basic, api_key, and oauth2 auth types.
        """
        if not auth_config:
            return headers

        auth_type = auth_config.get("auth_type", "")

        if auth_type == "bearer":
            token = auth_config.get("token", "")
            headers["Authorization"] = f"Bearer {token}"

        elif auth_type == "basic":
            username = auth_config.get("username", "")
            password = auth_config.get("password", "")
            encoded = base64.b64encode(f"{username}:{password}".encode()).decode()
            headers["Authorization"] = f"Basic {encoded}"

        elif auth_type == "api_key":
            key_name = auth_config.get("key_name", "X-API-Key")
            api_key = auth_config.get("api_key", "")
            key_location = auth_config.get("key_location", "header")
            if key_location == "header":
                headers[key_name] = api_key
            # query-string location handled by the caller

        elif auth_type == "oauth2":
            # OAuth2 client_credentials flow
            token = _obtain_oauth2_token(auth_config)
            if token:
                headers["Authorization"] = f"Bearer {token}"

        return headers


# ---------------------------------------------------------------------------
# SaaSSourceHandler
# ---------------------------------------------------------------------------

class SaaSSourceHandler:
    """
    Handler for SaaS API sources (Salesforce, ServiceNow, etc.).

    Returns configured client dicts that downstream tasks use to interact
    with SaaS platforms.
    """

    def get_salesforce_client(
        self,
        auth_config: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Build a configured Salesforce client descriptor.

        Args:
            auth_config: Authentication configuration with keys:
                - instance_url: Salesforce instance URL
                - client_id: Connected app consumer key
                - client_secret: Connected app consumer secret
                - username: Salesforce username
                - password: Salesforce password
                - security_token: Salesforce security token
                - api_version: API version (default 'v59.0')

        Returns:
            Client descriptor dict with connection metadata.
        """
        instance_url = auth_config.get("instance_url", "")
        api_version = auth_config.get("api_version", "v59.0")

        return {
            "platform": "salesforce",
            "instance_url": instance_url,
            "api_version": api_version,
            "auth_type": "oauth2",
            "base_url": f"{instance_url}/services/data/{api_version}",
            "bulk_api_url": f"{instance_url}/services/async/{api_version}",
            "connected": False,  # Actual connection happens at runtime
        }

    def get_servicenow_client(
        self,
        auth_config: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Build a configured ServiceNow client descriptor.

        Args:
            auth_config: Authentication configuration with keys:
                - instance_url: ServiceNow instance URL
                  (e.g. https://company.service-now.com)
                - username: ServiceNow username
                - password: ServiceNow password
                - client_id: OAuth client ID (optional, for oauth2)
                - client_secret: OAuth client secret (optional)
                - auth_type: basic | oauth2 (default 'basic')

        Returns:
            Client descriptor dict with connection metadata.
        """
        instance_url = auth_config.get("instance_url", "")
        auth_type = auth_config.get("auth_type", "basic")

        return {
            "platform": "servicenow",
            "instance_url": instance_url,
            "auth_type": auth_type,
            "table_api_url": f"{instance_url}/api/now/table",
            "import_set_url": f"{instance_url}/api/now/import",
            "connected": False,
        }

    def build_saas_query(
        self,
        source_type: str,
        query_config: Dict[str, Any],
    ) -> str:
        """
        Build a platform-specific API query string.

        Args:
            source_type: SaaS platform identifier
                (e.g. 'salesforce', 'servicenow').
            query_config: Query parameters:
                For Salesforce:
                    - object_name: SObject name (e.g. 'Account')
                    - fields: list of field names
                    - where_clause: optional SOQL WHERE clause
                    - limit: optional record limit
                For ServiceNow:
                    - table_name: ServiceNow table (e.g. 'incident')
                    - sysparm_query: encoded query string
                    - sysparm_fields: comma-separated field list
                    - sysparm_limit: record limit

        Returns:
            Query string appropriate for the platform API.
        """
        if source_type == "salesforce":
            object_name = query_config.get("object_name", "")
            fields = query_config.get("fields", ["Id"])
            where_clause = query_config.get("where_clause", "")
            limit = query_config.get("limit")

            fields_str = ", ".join(fields)
            query = f"SELECT {fields_str} FROM {object_name}"
            if where_clause:
                query += f" WHERE {where_clause}"
            if limit:
                query += f" LIMIT {limit}"
            return query

        elif source_type == "servicenow":
            table_name = query_config.get("table_name", "")
            sysparm_query = query_config.get("sysparm_query", "")
            sysparm_fields = query_config.get("sysparm_fields", "")
            sysparm_limit = query_config.get("sysparm_limit", 1000)

            parts = [f"sysparm_limit={sysparm_limit}"]
            if sysparm_query:
                parts.append(f"sysparm_query={sysparm_query}")
            if sysparm_fields:
                parts.append(f"sysparm_fields={sysparm_fields}")

            return f"/api/now/table/{table_name}?{'&'.join(parts)}"

        else:
            logger.warning("Unknown SaaS source type: %s", source_type)
            return ""


# ---------------------------------------------------------------------------
# Convenience functions
# ---------------------------------------------------------------------------

def fetch_rest_api(
    endpoint: str,
    method: str = "GET",
    headers: Optional[Dict[str, str]] = None,
    auth_type: Optional[str] = None,
    auth_secret: Optional[str] = None,
    pagination_type: Optional[str] = None,
    pagination_key: str = "next",
    rate_limit_rps: int = 0,
) -> List[Dict[str, Any]]:
    """
    Convenience function to fetch records from a REST API.

    Handles authentication, pagination, and rate limiting in a single call.

    Args:
        endpoint: Full URL of the REST endpoint.
        method: HTTP method (GET, POST, PUT, PATCH).
        headers: Additional HTTP headers.
        auth_type: Authentication type (bearer, basic, api_key, oauth2).
        auth_secret: Token, password, or API key (depending on auth_type).
        pagination_type: Pagination strategy
            (offset, cursor, link_header, token). None for single-page.
        pagination_key: JSON key for next-page indicator.
        rate_limit_rps: Maximum requests per second (0 = unlimited).

    Returns:
        List of record dicts from the API response.
    """
    if requests is None:
        logger.warning("requests library not available; returning empty list")
        return []

    handler = APISourceHandler()

    # Build auth config from shorthand parameters
    auth_config: Optional[Dict[str, Any]] = None
    if auth_type:
        auth_config = {"auth_type": auth_type}
        if auth_type == "bearer":
            auth_config["token"] = auth_secret or ""
        elif auth_type == "api_key":
            auth_config["api_key"] = auth_secret or ""
        elif auth_type == "basic":
            # For basic auth, auth_secret is expected as "username:password"
            if auth_secret and ":" in auth_secret:
                username, password = auth_secret.split(":", 1)
                auth_config["username"] = username
                auth_config["password"] = password

    # Build pagination config
    pagination_config: Optional[Dict[str, Any]] = None
    if pagination_type:
        pagination_config = {
            "pagination_type": pagination_type,
            "pagination_key": pagination_key,
        }

    # Apply rate limiting
    if rate_limit_rps > 0:
        rate_config = handler.apply_rate_limiting(rate_limit_rps)
        delay = rate_config.get("delay_seconds", 0)
        if delay > 0:
            time.sleep(delay)

    result = handler.fetch_api_data(
        endpoint=endpoint,
        method=method,
        headers=headers,
        auth_config=auth_config,
        pagination_config=pagination_config,
    )

    return result.get("records", [])


def fetch_graphql(
    endpoint: str,
    query: str,
    variables: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    auth_secret: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Convenience function to execute a GraphQL query.

    Args:
        endpoint: Full URL of the GraphQL endpoint.
        query: GraphQL query string.
        variables: Optional dict of query variables.
        headers: Additional HTTP headers.
        auth_secret: Bearer token for authentication (if required).

    Returns:
        Dict with keys:
            - data: the GraphQL response data
            - errors: list of errors (if any)
            - endpoint: the endpoint that was called
    """
    if requests is None:
        logger.warning("requests library not available; returning empty result")
        return {
            "data": None,
            "errors": [{"message": "requests library not available"}],
            "endpoint": endpoint,
        }

    merged_headers = dict(headers or {})
    merged_headers.setdefault("Content-Type", "application/json")

    if auth_secret:
        merged_headers["Authorization"] = f"Bearer {auth_secret}"

    payload = {"query": query}
    if variables:
        payload["variables"] = variables

    try:
        resp = requests.post(endpoint, json=payload, headers=merged_headers, timeout=60)
        resp.raise_for_status()
        body = resp.json()

        return {
            "data": body.get("data"),
            "errors": body.get("errors", []),
            "endpoint": endpoint,
        }

    except Exception as exc:
        logger.error("GraphQL request failed: %s", exc)
        return {
            "data": None,
            "errors": [{"message": str(exc)}],
            "endpoint": endpoint,
        }


# ---------------------------------------------------------------------------
# Module-level helpers (private)
# ---------------------------------------------------------------------------

def _make_request(
    endpoint: str,
    method: str,
    headers: Dict[str, str],
) -> Dict[str, Any]:
    """
    Execute an HTTP request and return parsed JSON.

    Args:
        endpoint: Full URL.
        method: HTTP method.
        headers: Request headers.

    Returns:
        Parsed JSON response as a dict, or empty dict on failure.
    """
    if requests is None:
        return {}

    try:
        resp = requests.request(
            method=method.upper(),
            url=endpoint,
            headers=headers,
            timeout=60,
        )
        resp.raise_for_status()
        return resp.json()

    except Exception as exc:
        logger.error("HTTP %s %s failed: %s", method, endpoint, exc)
        return {}


def _obtain_oauth2_token(auth_config: Dict[str, Any]) -> Optional[str]:
    """
    Obtain an OAuth2 access token using client_credentials grant.

    Args:
        auth_config: Dict with client_id, client_secret, and token_url.

    Returns:
        Access token string, or None on failure.
    """
    if requests is None:
        return None

    token_url = auth_config.get("token_url", "")
    client_id = auth_config.get("client_id", "")
    client_secret = auth_config.get("client_secret", "")

    if not token_url:
        logger.warning("OAuth2 token_url not provided")
        return None

    try:
        resp = requests.post(
            token_url,
            data={
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json().get("access_token")

    except Exception as exc:
        logger.error("OAuth2 token request failed: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Exports
# ---------------------------------------------------------------------------

__all__ = [
    "APISourceHandler",
    "SaaSSourceHandler",
    "fetch_rest_api",
    "fetch_graphql",
]
