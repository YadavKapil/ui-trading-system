"""Canonicalized pre-import filesystem allowlist and access ledger."""

from __future__ import annotations

import builtins
import contextlib
import io
import _io
import importlib.machinery
import importlib
import os
import site
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any


class UnexpectedFilesystemAccess(PermissionError):
    """Raised when replay code attempts an undeclared filesystem access."""


class FilesystemGuard:
    def __init__(self, *, sandbox_root: Path) -> None:
        self.sandbox_root = sandbox_root.resolve()
        self._resolving = False
        roots = {Path(sys.base_prefix).resolve(), Path(sys.prefix).resolve()}
        roots.update(Path(value).resolve() for value in site.getsitepackages())
        user_site = site.getusersitepackages()
        if user_site:
            roots.add(Path(user_site).resolve())
        self.read_roots = tuple(sorted(roots, key=lambda value: str(value)))
        self.ledger: list[dict[str, str]] = []

    def _resolve(self, value: Any) -> Path | None:
        if isinstance(value, int):
            return None
        absolute = os.path.abspath(os.fspath(value))
        if self._resolving:
            return Path(absolute)
        self._resolving = True
        try:
            return Path(os.path.realpath(absolute))
        finally:
            self._resolving = False

    @staticmethod
    def _is_write_mode(mode: str) -> bool:
        return any(flag in mode for flag in ("w", "a", "x", "+"))

    def _allowed_read(self, path: Path) -> bool:
        return self._inside(path, self.sandbox_root) or any(self._inside(path, root) for root in self.read_roots)

    @staticmethod
    def _inside(path: Path, root: Path) -> bool:
        try:
            path.relative_to(root)
            return True
        except ValueError:
            return False

    def _record(self, operation: str, path: Path, *, allowed: bool) -> None:
        if self._inside(path, self.sandbox_root):
            display = str(path.relative_to(self.sandbox_root))
        elif allowed:
            display = "<package-read>"
        else:
            display = "<denied-external-path>"
        self.ledger.append({"operation": operation, "path": display, "allowed": str(allowed).lower()})

    def check(self, value: Any, *, operation: str, write: bool) -> None:
        path = self._resolve(value)
        if path is None:
            return
        allowed = self._inside(path, self.sandbox_root) if write else self._allowed_read(path)
        self._record(operation, path, allowed=allowed)
        if not allowed:
            raise UnexpectedFilesystemAccess(f"Replay denied {operation} outside the sandbox")

    @staticmethod
    def reject_dir_fds(kwargs: dict[str, Any]) -> None:
        if any(kwargs.get(name) is not None for name in ("dir_fd", "src_dir_fd", "dst_dir_fd")):
            raise UnexpectedFilesystemAccess("Replay denies descriptor-relative filesystem operations")

    @contextlib.contextmanager
    def installed(self) -> Iterator["FilesystemGuard"]:
        original_builtin_open = builtins.open
        original_io_open = io.open
        original_raw_open = _io.open
        original_file_io = _io.FileIO
        original_os_open = os.open
        original_listdir = os.listdir
        original_scandir = os.scandir
        original_remove = os.remove
        original_unlink = os.unlink
        original_rename = os.rename
        original_replace = os.replace
        original_mkdir = os.mkdir
        original_makedirs = os.makedirs
        original_rmdir = os.rmdir
        original_stat = os.stat
        original_lstat = os.lstat
        original_readlink = os.readlink
        xattr_names = [
            name
            for name in ("getxattr", "listxattr", "setxattr", "removexattr")
            if hasattr(os, name)
        ]
        original_xattrs = {name: getattr(os, name) for name in xattr_names}
        single_mutators = {
            name: getattr(os, name)
            for name in ("truncate", "chmod", "chown", "lchmod", "utime")
            if hasattr(os, name)
        }
        dual_mutators = {
            name: getattr(os, name)
            for name in ("link", "symlink")
            if hasattr(os, name)
        }
        original_loader_get_data = importlib.machinery.SourceFileLoader.get_data
        platform = importlib.import_module(os.name)

        def guarded_open(file, mode="r", *args, **kwargs):
            self.check(file, operation=f"open:{mode}", write=self._is_write_mode(str(mode)))
            return original_builtin_open(file, mode, *args, **kwargs)

        def guarded_io_open(file, mode="r", *args, **kwargs):
            self.check(file, operation=f"io.open:{mode}", write=self._is_write_mode(str(mode)))
            return original_io_open(file, mode, *args, **kwargs)

        def guarded_raw_open(file, mode="r", *args, **kwargs):
            self.check(file, operation=f"_io.open:{mode}", write=self._is_write_mode(str(mode)))
            return original_raw_open(file, mode, *args, **kwargs)

        def guarded_file_io(file, mode="r", *args, **kwargs):
            self.check(file, operation=f"_io.FileIO:{mode}", write=self._is_write_mode(str(mode)))
            return original_file_io(file, mode, *args, **kwargs)

        def guarded_os_open(path, flags, *args, **kwargs):
            self.reject_dir_fds(kwargs)
            write_flags = os.O_WRONLY | os.O_RDWR | os.O_APPEND | os.O_CREAT | os.O_TRUNC
            self.check(path, operation="os.open", write=bool(flags & write_flags))
            return original_os_open(path, flags, *args, **kwargs)

        def guarded_listdir(path="."):
            if isinstance(path, int):
                raise UnexpectedFilesystemAccess("Replay denies descriptor-based listdir")
            self.check(path, operation="listdir", write=False)
            return original_listdir(path)

        def guarded_scandir(path="."):
            if isinstance(path, int):
                raise UnexpectedFilesystemAccess("Replay denies descriptor-based scandir")
            self.check(path, operation="scandir", write=False)
            return original_scandir(path)

        def guarded_remove(path, *args, **kwargs):
            self.reject_dir_fds(kwargs)
            self.check(path, operation="remove", write=True)
            return original_remove(path, *args, **kwargs)

        def guarded_unlink(path, *args, **kwargs):
            self.reject_dir_fds(kwargs)
            self.check(path, operation="unlink", write=True)
            return original_unlink(path, *args, **kwargs)

        def guarded_rename(source, destination, *args, **kwargs):
            self.reject_dir_fds(kwargs)
            self.check(source, operation="rename:source", write=True)
            self.check(destination, operation="rename:destination", write=True)
            return original_rename(source, destination, *args, **kwargs)

        def guarded_replace(source, destination, *args, **kwargs):
            self.reject_dir_fds(kwargs)
            self.check(source, operation="replace:source", write=True)
            self.check(destination, operation="replace:destination", write=True)
            return original_replace(source, destination, *args, **kwargs)

        def guarded_mkdir(path, *args, **kwargs):
            self.reject_dir_fds(kwargs)
            self.check(path, operation="mkdir", write=True)
            return original_mkdir(path, *args, **kwargs)

        def guarded_makedirs(name, *args, **kwargs):
            self.check(name, operation="makedirs", write=True)
            return original_makedirs(name, *args, **kwargs)

        def guarded_rmdir(path, *args, **kwargs):
            self.reject_dir_fds(kwargs)
            self.check(path, operation="rmdir", write=True)
            return original_rmdir(path, *args, **kwargs)

        def guarded_stat(path, *args, **kwargs):
            self.reject_dir_fds(kwargs)
            self.check(path, operation="stat", write=False)
            return original_stat(path, *args, **kwargs)

        def guarded_lstat(path, *args, **kwargs):
            self.reject_dir_fds(kwargs)
            self.check(path, operation="lstat", write=False)
            return original_lstat(path, *args, **kwargs)

        def guarded_readlink(path, *args, **kwargs):
            self.reject_dir_fds(kwargs)
            self.check(path, operation="readlink", write=False)
            return original_readlink(path, *args, **kwargs)

        def guarded_loader_get_data(loader, path):
            self.check(path, operation="import.get_data", write=False)
            return original_loader_get_data(loader, path)

        builtins.open = guarded_open
        io.open = guarded_io_open
        _io.open = guarded_raw_open
        _io.FileIO = guarded_file_io
        os.open = guarded_os_open
        os.listdir = guarded_listdir
        os.scandir = guarded_scandir
        os.remove = guarded_remove
        os.unlink = guarded_unlink
        os.rename = guarded_rename
        os.replace = guarded_replace
        os.mkdir = guarded_mkdir
        os.makedirs = guarded_makedirs
        os.rmdir = guarded_rmdir
        os.stat = guarded_stat
        os.lstat = guarded_lstat
        os.readlink = guarded_readlink
        importlib.machinery.SourceFileLoader.get_data = guarded_loader_get_data
        for name, original in single_mutators.items():
            def single_guard(path, *args, _name=name, _original=original, **kwargs):
                self.reject_dir_fds(kwargs)
                self.check(path, operation=_name, write=True)
                return _original(path, *args, **kwargs)

            setattr(os, name, single_guard)
        for name, original in dual_mutators.items():
            def dual_guard(source, destination, *args, _name=name, _original=original, **kwargs):
                self.reject_dir_fds(kwargs)
                self.check(source, operation=f"{_name}:source", write=True)
                self.check(destination, operation=f"{_name}:destination", write=True)
                return _original(source, destination, *args, **kwargs)

            setattr(os, name, dual_guard)
        for name, original in original_xattrs.items():
            def xattr_guard(path, *args, _name=name, _original=original, **kwargs):
                self.reject_dir_fds(kwargs)
                self.check(
                    path,
                    operation=_name,
                    write=_name in {"setxattr", "removexattr"},
                )
                return _original(path, *args, **kwargs)

            setattr(os, name, xattr_guard)
        guarded_platform_names = {
            name
            for name in (
                "open",
                "listdir",
                "scandir",
                "remove",
                "unlink",
                "rename",
                "replace",
                "mkdir",
                "rmdir",
                "stat",
                "lstat",
                "readlink",
                *xattr_names,
                *single_mutators.keys(),
                *dual_mutators.keys(),
            )
            if hasattr(platform, name) and hasattr(os, name)
        }
        original_platform = {name: getattr(platform, name) for name in guarded_platform_names}
        for name in guarded_platform_names:
            setattr(platform, name, getattr(os, name))
        try:
            yield self
        finally:
            for name, original in original_platform.items():
                setattr(platform, name, original)
            for name, original in dual_mutators.items():
                setattr(os, name, original)
            for name, original in single_mutators.items():
                setattr(os, name, original)
            for name, original in original_xattrs.items():
                setattr(os, name, original)
            importlib.machinery.SourceFileLoader.get_data = original_loader_get_data
            os.rmdir = original_rmdir
            os.readlink = original_readlink
            os.lstat = original_lstat
            os.stat = original_stat
            os.makedirs = original_makedirs
            os.mkdir = original_mkdir
            os.replace = original_replace
            os.rename = original_rename
            os.unlink = original_unlink
            os.remove = original_remove
            os.scandir = original_scandir
            os.listdir = original_listdir
            os.open = original_os_open
            _io.FileIO = original_file_io
            _io.open = original_raw_open
            io.open = original_io_open
            builtins.open = original_builtin_open
