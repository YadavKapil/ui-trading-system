"""Fail-closed outbound network guard for replay bootstrap."""

from __future__ import annotations

import contextlib
import _socket
import socket
import urllib.request
from collections.abc import Callable, Iterator

import requests

from .tracing import TraceRecorder


class UnexpectedNetworkAccess(RuntimeError):
    """Raised before an unregistered outbound network operation executes."""


class NetworkGuard:
    def __init__(self, *, trace: TraceRecorder, now: Callable) -> None:
        self.trace = trace
        self.now = now

    def _deny(self, category: str):
        self.trace.add(
            "UNEXPECTED_NETWORK_ACCESS",
            at=self.now(),
            operation=category,
            details={"blocked": True},
        )
        raise UnexpectedNetworkAccess(f"Replay denied outbound {category}")

    @contextlib.contextmanager
    def installed(self) -> Iterator[None]:
        originals = {
            "socket": socket.socket,
            "socket_type": socket.SocketType,
            "raw_socket": _socket.socket,
            "raw_socket_type": _socket.SocketType,
            "socketpair": getattr(socket, "socketpair", None),
            "raw_socketpair": getattr(_socket, "socketpair", None),
            "create_connection": socket.create_connection,
            "getaddrinfo": socket.getaddrinfo,
            "raw_getaddrinfo": _socket.getaddrinfo,
            "requests": requests.sessions.Session.request,
            "urlopen": urllib.request.urlopen,
        }
        resolver_names = [
            name
            for name in (
                "gethostbyname",
                "gethostbyname_ex",
                "gethostbyaddr",
                "getnameinfo",
            )
            if hasattr(socket, name)
        ]
        raw_resolver_names = [name for name in resolver_names if hasattr(_socket, name)]
        original_resolvers = {name: getattr(socket, name) for name in resolver_names}
        original_raw_resolvers = {name: getattr(_socket, name) for name in raw_resolver_names}

        class DeniedSocket:
            def __init__(_self, *args, **kwargs):
                self._deny("socket")

        socket.socket = DeniedSocket  # type: ignore[assignment]
        socket.SocketType = DeniedSocket  # type: ignore[assignment]
        _socket.socket = DeniedSocket  # type: ignore[assignment]
        _socket.SocketType = DeniedSocket  # type: ignore[assignment]
        if originals["socketpair"] is not None:
            socket.socketpair = lambda *args, **kwargs: self._deny("socketpair")  # type: ignore[assignment]
        if originals["raw_socketpair"] is not None:
            _socket.socketpair = lambda *args, **kwargs: self._deny("socketpair")  # type: ignore[assignment]
        socket.create_connection = lambda *args, **kwargs: self._deny("socket.create_connection")  # type: ignore[assignment]
        socket.getaddrinfo = lambda *args, **kwargs: self._deny("dns")  # type: ignore[assignment]
        _socket.getaddrinfo = lambda *args, **kwargs: self._deny("dns")  # type: ignore[assignment]
        for name in resolver_names:
            setattr(socket, name, lambda *args, _name=name, **kwargs: self._deny(f"dns.{_name}"))
        for name in raw_resolver_names:
            setattr(_socket, name, lambda *args, _name=name, **kwargs: self._deny(f"dns.{_name}"))
        requests.sessions.Session.request = lambda *_args, **_kwargs: self._deny("http")  # type: ignore[assignment]
        urllib.request.urlopen = lambda *args, **kwargs: self._deny("http")  # type: ignore[assignment]
        try:
            yield
        finally:
            urllib.request.urlopen = originals["urlopen"]  # type: ignore[assignment]
            requests.sessions.Session.request = originals["requests"]  # type: ignore[assignment]
            for name, value in original_raw_resolvers.items():
                setattr(_socket, name, value)
            for name, value in original_resolvers.items():
                setattr(socket, name, value)
            _socket.getaddrinfo = originals["raw_getaddrinfo"]  # type: ignore[assignment]
            socket.getaddrinfo = originals["getaddrinfo"]  # type: ignore[assignment]
            socket.create_connection = originals["create_connection"]  # type: ignore[assignment]
            if originals["raw_socketpair"] is not None:
                _socket.socketpair = originals["raw_socketpair"]  # type: ignore[assignment]
            if originals["socketpair"] is not None:
                socket.socketpair = originals["socketpair"]  # type: ignore[assignment]
            _socket.socket = originals["raw_socket"]  # type: ignore[assignment]
            _socket.SocketType = originals["raw_socket_type"]  # type: ignore[assignment]
            socket.SocketType = originals["socket_type"]  # type: ignore[assignment]
            socket.socket = originals["socket"]  # type: ignore[assignment]
