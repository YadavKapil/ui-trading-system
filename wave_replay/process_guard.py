"""Deny child-process escape from deterministic replay contexts."""

from __future__ import annotations

import concurrent.futures
import contextlib
import importlib
import multiprocessing
import multiprocessing.process
import os
import subprocess
from collections.abc import Callable, Iterator

from .tracing import TraceRecorder


class UnexpectedProcessAccess(RuntimeError):
    """Raised when replay code attempts to create a child process."""


class ProcessGuard:
    def __init__(self, *, trace: TraceRecorder, now: Callable) -> None:
        self.trace = trace
        self.now = now

    def _deny(self, category: str):
        self.trace.add(
            "UNEXPECTED_PROCESS_ACCESS",
            at=self.now(),
            operation=category,
            details={"blocked": True},
        )
        raise UnexpectedProcessAccess(f"Replay denied {category}")

    @contextlib.contextmanager
    def installed(self) -> Iterator[None]:
        original_popen = subprocess.Popen
        original_run = subprocess.run
        original_call = subprocess.call
        original_check_call = subprocess.check_call
        original_check_output = subprocess.check_output
        original_os_system = os.system
        original_os_popen = os.popen
        original_process_start = multiprocessing.Process.start
        original_base_process_start = multiprocessing.process.BaseProcess.start
        original_pool_submit = concurrent.futures.ProcessPoolExecutor.submit
        try:
            import _winapi
        except ImportError:
            _winapi = None
            original_create_process = None
        else:
            original_create_process = _winapi.CreateProcess
        spawn_names = [
            name
            for name in dir(os)
            if (
                name.startswith("spawn")
                or name in {"posix_spawn", "posix_spawnp"}
            )
            and callable(getattr(os, name))
        ]
        original_spawns = {name: getattr(os, name) for name in spawn_names}
        exec_names = [
            name
            for name in dir(os)
            if (name.startswith("exec") or name in {"fork", "forkpty", "startfile"})
            and callable(getattr(os, name))
        ]
        original_execs = {name: getattr(os, name) for name in exec_names}
        platform = importlib.import_module(os.name)

        subprocess.Popen = lambda *args, **kwargs: self._deny("subprocess.Popen")  # type: ignore[assignment]
        subprocess.run = lambda *args, **kwargs: self._deny("subprocess.run")  # type: ignore[assignment]
        subprocess.call = lambda *args, **kwargs: self._deny("subprocess.call")  # type: ignore[assignment]
        subprocess.check_call = lambda *args, **kwargs: self._deny("subprocess.check_call")  # type: ignore[assignment]
        subprocess.check_output = lambda *args, **kwargs: self._deny("subprocess.check_output")  # type: ignore[assignment]
        os.system = lambda *args, **kwargs: self._deny("os.system")
        os.popen = lambda *args, **kwargs: self._deny("os.popen")  # type: ignore[assignment]
        multiprocessing.Process.start = lambda *_args, **_kwargs: self._deny("multiprocessing.Process.start")  # type: ignore[assignment]
        multiprocessing.process.BaseProcess.start = lambda *_args, **_kwargs: self._deny("multiprocessing.BaseProcess.start")  # type: ignore[assignment]
        concurrent.futures.ProcessPoolExecutor.submit = lambda *_args, **_kwargs: self._deny("ProcessPoolExecutor")  # type: ignore[assignment]
        if _winapi is not None:
            _winapi.CreateProcess = lambda *args, **kwargs: self._deny("_winapi.CreateProcess")
        for name in spawn_names:
            setattr(os, name, lambda *args, _name=name, **kwargs: self._deny(f"os.{_name}"))
        for name in exec_names:
            setattr(os, name, lambda *args, _name=name, **kwargs: self._deny(f"os.{_name}"))
        platform_names = {
            name
            for name in (
                "system",
                "popen",
                *spawn_names,
                *exec_names,
            )
            if hasattr(platform, name) and hasattr(os, name)
        }
        original_platform = {name: getattr(platform, name) for name in platform_names}
        for name in platform_names:
            setattr(platform, name, getattr(os, name))
        try:
            yield
        finally:
            for name, value in original_platform.items():
                setattr(platform, name, value)
            if _winapi is not None and original_create_process is not None:
                _winapi.CreateProcess = original_create_process
            for name, value in original_execs.items():
                setattr(os, name, value)
            for name, value in original_spawns.items():
                setattr(os, name, value)
            concurrent.futures.ProcessPoolExecutor.submit = original_pool_submit
            multiprocessing.Process.start = original_process_start
            multiprocessing.process.BaseProcess.start = original_base_process_start
            os.popen = original_os_popen
            os.system = original_os_system
            subprocess.check_output = original_check_output
            subprocess.check_call = original_check_call
            subprocess.call = original_call
            subprocess.run = original_run
            subprocess.Popen = original_popen
