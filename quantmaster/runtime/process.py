"""Bounded process-start helpers for transient Windows launch failures."""

from __future__ import annotations

import ntpath
import os
import signal
import subprocess
import sys
import tempfile
import time
from collections.abc import Sequence
from dataclasses import dataclass
from os import PathLike
from typing import Any

_TRANSIENT_WINDOWS_ERRORS = {5, 32}


class ProcessLimitError(RuntimeError):
    """A restricted child exceeded an operating-system resource boundary."""


@dataclass(frozen=True)
class ProcessLimits:
    memory_bytes: int
    cpu_seconds: int
    output_bytes: int
    max_processes: int = 1
    file_bytes: int | None = None


def _prepare_windows_venv_launch(
    command: Sequence[str | PathLike[str]],
    child_env: dict[str, str],
    *,
    platform: str | None = None,
    executable: str | None = None,
    base_executable: str | None = None,
    search_path: Sequence[str] | None = None,
) -> tuple[list[str | PathLike[str]], dict[str, str]]:
    """Bypass a Windows venv redirector without weakening the Job Object limit.

    Windows virtual-environment launchers create the base interpreter as a
    second process. Assigning the launcher to a one-process Job Object therefore
    prevents Python itself from starting. Running the base interpreter with the
    current environment's import path keeps the worker functional while leaving
    no spare Job Object slot for untrusted descendants.
    """
    launch_command = list(command)
    prepared_env = dict(child_env)
    current_platform = os.name if platform is None else platform
    if current_platform != "nt" or not launch_command:
        return launch_command, prepared_env

    current_executable = sys.executable if executable is None else executable
    current_base = (
        getattr(sys, "_base_executable", current_executable)
        if base_executable is None
        else base_executable
    )
    requested = ntpath.normcase(ntpath.abspath(os.fspath(launch_command[0])))
    current = ntpath.normcase(ntpath.abspath(current_executable))
    base = ntpath.normcase(ntpath.abspath(current_base))
    if requested != current or base == current:
        return launch_command, prepared_env

    launch_command[0] = current_base
    paths = [entry for entry in (sys.path if search_path is None else search_path) if entry]
    paths.extend(
        entry for entry in prepared_env.get("PYTHONPATH", "").split(";") if entry
    )
    prepared_env["PYTHONPATH"] = ";".join(dict.fromkeys(paths))
    return launch_command, prepared_env


class _WindowsJob:
    """Minimal Job Object wrapper kept private to avoid a pywin32 dependency."""

    def __init__(self, process: subprocess.Popen[Any], limits: ProcessLimits) -> None:
        import ctypes
        from ctypes import wintypes

        class BasicLimitInformation(ctypes.Structure):
            _fields_ = [
                ("PerProcessUserTimeLimit", ctypes.c_longlong),
                ("PerJobUserTimeLimit", ctypes.c_longlong),
                ("LimitFlags", wintypes.DWORD),
                ("MinimumWorkingSetSize", ctypes.c_size_t),
                ("MaximumWorkingSetSize", ctypes.c_size_t),
                ("ActiveProcessLimit", wintypes.DWORD),
                ("Affinity", ctypes.c_size_t),
                ("PriorityClass", wintypes.DWORD),
                ("SchedulingClass", wintypes.DWORD),
            ]

        class IoCounters(ctypes.Structure):
            _fields_ = [
                ("ReadOperationCount", ctypes.c_ulonglong),
                ("WriteOperationCount", ctypes.c_ulonglong),
                ("OtherOperationCount", ctypes.c_ulonglong),
                ("ReadTransferCount", ctypes.c_ulonglong),
                ("WriteTransferCount", ctypes.c_ulonglong),
                ("OtherTransferCount", ctypes.c_ulonglong),
            ]

        class ExtendedLimitInformation(ctypes.Structure):
            _fields_ = [
                ("BasicLimitInformation", BasicLimitInformation),
                ("IoInfo", IoCounters),
                ("ProcessMemoryLimit", ctypes.c_size_t),
                ("JobMemoryLimit", ctypes.c_size_t),
                ("PeakProcessMemoryUsed", ctypes.c_size_t),
                ("PeakJobMemoryUsed", ctypes.c_size_t),
            ]

        kernel = ctypes.WinDLL(  # type: ignore[attr-defined]
            "kernel32", use_last_error=True,
        )
        kernel.CreateJobObjectW.argtypes = [ctypes.c_void_p, wintypes.LPCWSTR]
        kernel.CreateJobObjectW.restype = wintypes.HANDLE
        kernel.SetInformationJobObject.argtypes = [
            wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD,
        ]
        kernel.SetInformationJobObject.restype = wintypes.BOOL
        kernel.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
        kernel.AssignProcessToJobObject.restype = wintypes.BOOL
        kernel.TerminateJobObject.argtypes = [wintypes.HANDLE, wintypes.UINT]
        kernel.TerminateJobObject.restype = wintypes.BOOL
        kernel.CloseHandle.argtypes = [wintypes.HANDLE]
        kernel.CloseHandle.restype = wintypes.BOOL

        handle = kernel.CreateJobObjectW(None, None)
        if not handle:
            raise OSError(
                ctypes.get_last_error(),  # type: ignore[attr-defined]
                "CreateJobObjectW failed",
            )
        self._kernel = kernel
        self._handle = handle
        try:
            info = ExtendedLimitInformation()
            # 100-ns units; Job time includes all processes in the job.
            info.BasicLimitInformation.PerJobUserTimeLimit = (
                max(1, int(limits.cpu_seconds)) * 10_000_000
            )
            info.BasicLimitInformation.ActiveProcessLimit = max(
                1, int(limits.max_processes)
            )
            info.ProcessMemoryLimit = max(16 * 1024 * 1024, int(limits.memory_bytes))
            info.BasicLimitInformation.LimitFlags = (
                0x00000004  # JOB_OBJECT_LIMIT_JOB_TIME
                | 0x00000008  # JOB_OBJECT_LIMIT_ACTIVE_PROCESS
                | 0x00000100  # JOB_OBJECT_LIMIT_PROCESS_MEMORY
                | 0x00002000  # JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
                | 0x00000400  # JOB_OBJECT_LIMIT_DIE_ON_UNHANDLED_EXCEPTION
            )
            if not kernel.SetInformationJobObject(
                handle, 9, ctypes.byref(info), ctypes.sizeof(info),
            ):
                raise OSError(
                    ctypes.get_last_error(),  # type: ignore[attr-defined]
                    "SetInformationJobObject failed",
                )
            process_handle = wintypes.HANDLE(int(process._handle))  # type: ignore[attr-defined]
            if not kernel.AssignProcessToJobObject(handle, process_handle):
                raise OSError(
                    ctypes.get_last_error(),  # type: ignore[attr-defined]
                    "AssignProcessToJobObject failed",
                )
        except Exception:
            self.close()
            raise

    def terminate(self, exit_code: int = 1) -> None:
        if self._handle:
            self._kernel.TerminateJobObject(self._handle, exit_code)

    def close(self) -> None:
        if self._handle:
            self._kernel.CloseHandle(self._handle)
            self._handle = None


def _posix_limit_setup(limits: ProcessLimits):
    def apply() -> None:
        import resource

        memory = max(16 * 1024 * 1024, int(limits.memory_bytes))
        cpu = max(1, int(limits.cpu_seconds))
        output = max(1024, int(limits.file_bytes or limits.output_bytes))

        def cap(kind: int, requested: int) -> None:
            try:
                _, hard = resource.getrlimit(kind)  # type: ignore[attr-defined]
                value = (
                    requested
                    if hard == resource.RLIM_INFINITY  # type: ignore[attr-defined]
                    else min(requested, hard)
                )
                resource.setrlimit(kind, (value, value))  # type: ignore[attr-defined]
            except (OSError, ValueError):
                # Some macOS/POSIX runners expose constants whose limits cannot
                # be lowered in a pre-exec child. Other supported caps and the
                # process-group kill boundary remain active.
                pass

        cap(resource.RLIMIT_AS, memory)  # type: ignore[attr-defined]
        cap(resource.RLIMIT_CPU, cpu)  # type: ignore[attr-defined]
        cap(resource.RLIMIT_FSIZE, output)  # type: ignore[attr-defined]
        # Darwin applies RLIMIT_NPROC to the whole runner user and may reject a
        # value below the already-running process count. Linux applies it here;
        # macOS still gets bounded process-group cleanup on every exit path.
        if sys.platform != "darwin" and hasattr(resource, "RLIMIT_NPROC"):
            cap(resource.RLIMIT_NPROC, max(1, limits.max_processes))

    return apply


def run_restricted_process(
    command: Sequence[str | PathLike[str]],
    *,
    limits: ProcessLimits,
    timeout: float,
    env: dict[str, str] | None = None,
    cwd: str | PathLike[str] | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run a child with OS resource limits, process-tree termination and bounded logs."""
    preexec = _posix_limit_setup(limits) if os.name != "nt" else None
    child_env = dict(os.environ if env is None else env)
    # Native numeric runtimes commonly start a thread pool during import. Keep
    # it inside a one-process sandbox instead of letting RLIMIT_NPROC turn a
    # harmless import into a platform-dependent crash.
    for key in ("OPENBLAS_NUM_THREADS", "OMP_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS"):
        child_env[key] = "1"
    launch_command, child_env = _prepare_windows_venv_launch(command, child_env)
    with tempfile.TemporaryFile() as stdout_file, tempfile.TemporaryFile() as stderr_file:
        try:
            process = subprocess.Popen(
                launch_command, stdout=stdout_file, stderr=stderr_file, env=child_env, cwd=cwd,
                preexec_fn=preexec, start_new_session=os.name != "nt",
            )
        except OSError:
            raise
        job: _WindowsJob | None = None
        try:
            if os.name == "nt":
                try:
                    job = _WindowsJob(process, limits)
                except Exception:
                    process.kill()
                    process.wait(timeout=5)
                    raise
            deadline = time.monotonic() + max(0.01, float(timeout))
            while process.poll() is None:
                size = os.fstat(stdout_file.fileno()).st_size + os.fstat(stderr_file.fileno()).st_size
                if size > max(1024, int(limits.output_bytes)):
                    if job:
                        job.terminate(120)
                    elif os.name != "nt":
                        os.killpg(process.pid, signal.SIGKILL)  # type: ignore[attr-defined]
                    else:  # pragma: no cover - Windows always has a job here
                        process.kill()
                    process.wait(timeout=5)
                    raise ProcessLimitError("子进程输出超过安全上限")
                if time.monotonic() >= deadline:
                    if job:
                        job.terminate(121)
                    elif os.name != "nt":
                        os.killpg(process.pid, signal.SIGKILL)  # type: ignore[attr-defined]
                    else:  # pragma: no cover - Windows always has a job here
                        process.kill()
                    process.wait(timeout=5)
                    raise subprocess.TimeoutExpired(launch_command, timeout)
                time.sleep(0.02)
            stdout_file.seek(0)
            stderr_file.seek(0)
            stdout = stdout_file.read(max(1024, limits.output_bytes) + 1).decode(
                "utf-8", errors="replace",
            )
            stderr = stderr_file.read(max(1024, limits.output_bytes) + 1).decode(
                "utf-8", errors="replace",
            )
            if len(stdout.encode()) + len(stderr.encode()) > limits.output_bytes:
                raise ProcessLimitError("子进程输出超过安全上限")
            return subprocess.CompletedProcess(launch_command, process.returncode, stdout, stderr)
        finally:
            if process.poll() is None:
                if job:
                    job.terminate(122)
                else:
                    process.kill()
                process.wait(timeout=5)
            if job:
                job.close()


def run_process(
    command: Sequence[str | PathLike[str]],
    *,
    start_attempts: int = 4,
    retry_delay: float = 0.05,
    **kwargs: Any,
) -> subprocess.CompletedProcess[Any]:
    """Run a process, retrying only transient Windows CreateProcess failures.

    Error 5 (access denied) and 32 (sharing violation) can be produced briefly by
    endpoint scanners while a freshly started executable is inspected.  Runtime
    errors and non-Windows launch errors are deliberately not retried.
    """
    attempts = max(1, int(start_attempts))
    for attempt in range(attempts):
        try:
            return subprocess.run(list(command), **kwargs)
        except OSError as exc:
            retryable = getattr(exc, "winerror", None) in _TRANSIENT_WINDOWS_ERRORS
            if not retryable or attempt + 1 >= attempts:
                raise
            time.sleep(max(0.0, retry_delay) * (2**attempt))
    raise AssertionError("unreachable")
