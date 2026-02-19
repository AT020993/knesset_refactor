"""Minimal requests-compatible shim for environments without external requests.

This module implements the tiny subset of the ``requests`` API used by the CAP
PDF viewer and its tests:
- ``get(url, timeout=...)``
- ``Response.content``, ``Response.text``, ``Response.status_code``,
  ``Response.raise_for_status()``
- ``requests.exceptions`` namespace with ``Timeout``, ``SSLError``,
  ``ConnectionError``, and ``HTTPError``.

It intentionally avoids full feature parity with ``requests``.

Note: This module was originally ``src/requests.py`` but was moved here to
avoid shadowing the real ``requests`` package when PYTHONPATH includes ``src/``.
"""

from __future__ import annotations

from dataclasses import dataclass
import socket
import ssl
from typing import Any
import urllib.error
import urllib.request


class RequestException(Exception):
    """Base exception for request failures."""


class Timeout(RequestException):
    """Raised when a request times out."""


class SSLError(RequestException):
    """Raised when SSL/TLS setup fails."""


class ConnectionError(RequestException):
    """Raised when a connection cannot be established."""


class HTTPError(RequestException):
    """Raised for non-2xx HTTP status responses."""

    def __init__(self, message: str = "", response: "Response | None" = None):
        super().__init__(message)
        self.response = response


class _ExceptionsNamespace:
    RequestException = RequestException
    Timeout = Timeout
    SSLError = SSLError
    ConnectionError = ConnectionError
    HTTPError = HTTPError


exceptions = _ExceptionsNamespace()


@dataclass
class Response:
    """Small response object compatible with the subset we use."""

    status_code: int
    content: bytes
    url: str

    @property
    def text(self) -> str:
        return self.content.decode("utf-8", errors="replace")

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise HTTPError(f"HTTP {self.status_code}", response=self)


def get(url: str, timeout: float | int | None = None, **_: Any) -> Response:
    """Perform an HTTP GET request."""
    request = urllib.request.Request(url=url, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=timeout) as resp:
            status_code = int(getattr(resp, "status", 200))
            content = resp.read()
            return Response(status_code=status_code, content=content, url=url)
    except urllib.error.HTTPError as exc:
        body = exc.read() if hasattr(exc, "read") else b""
        response = Response(status_code=int(exc.code), content=body, url=url)
        raise HTTPError(f"HTTP {exc.code}", response=response) from exc
    except socket.timeout as exc:
        raise Timeout(str(exc)) from exc
    except ssl.SSLError as exc:
        raise SSLError(str(exc)) from exc
    except urllib.error.URLError as exc:
        raise ConnectionError(str(exc)) from exc
