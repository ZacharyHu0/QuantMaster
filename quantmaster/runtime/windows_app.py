"""Windows application identity and root process-tree ownership."""

from __future__ import annotations

import ctypes
import logging
import multiprocessing
import os
import shutil
import sys
import threading
from ctypes import wintypes
from multiprocessing import spawn
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

APP_USER_MODEL_ID = "QuantMaster.Personal"
APP_JOB_ENV = "QM_WINDOWS_APP_JOB_ROOT"
_ROOT_JOB: Any | None = None
_ROLE_EXECUTABLE_LOCK = threading.RLock()


class _BasicLimitInformation(ctypes.Structure):
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


class _IoCounters(ctypes.Structure):
    _fields_ = [
        ("ReadOperationCount", ctypes.c_ulonglong),
        ("WriteOperationCount", ctypes.c_ulonglong),
        ("OtherOperationCount", ctypes.c_ulonglong),
        ("ReadTransferCount", ctypes.c_ulonglong),
        ("WriteTransferCount", ctypes.c_ulonglong),
        ("OtherTransferCount", ctypes.c_ulonglong),
    ]


class _ExtendedLimitInformation(ctypes.Structure):
    _fields_ = [
        ("BasicLimitInformation", _BasicLimitInformation),
        ("IoInfo", _IoCounters),
        ("ProcessMemoryLimit", ctypes.c_size_t),
        ("JobMemoryLimit", ctypes.c_size_t),
        ("PeakProcessMemoryUsed", ctypes.c_size_t),
        ("PeakJobMemoryUsed", ctypes.c_size_t),
    ]


def _set_app_user_model_id() -> None:
    shell32 = ctypes.WinDLL("shell32", use_last_error=True)  # type: ignore[attr-defined]
    function = shell32.SetCurrentProcessExplicitAppUserModelID
    function.argtypes = [wintypes.LPCWSTR]
    function.restype = ctypes.c_long
    result = int(function(APP_USER_MODEL_ID))
    if result < 0:
        raise OSError(result, "SetCurrentProcessExplicitAppUserModelID failed")


def _create_root_job() -> Any:
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)  # type: ignore[attr-defined]
    kernel32.CreateJobObjectW.argtypes = [ctypes.c_void_p, wintypes.LPCWSTR]
    kernel32.CreateJobObjectW.restype = wintypes.HANDLE
    kernel32.SetInformationJobObject.argtypes = [
        wintypes.HANDLE,
        ctypes.c_int,
        ctypes.c_void_p,
        wintypes.DWORD,
    ]
    kernel32.SetInformationJobObject.restype = wintypes.BOOL
    kernel32.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
    kernel32.AssignProcessToJobObject.restype = wintypes.BOOL
    kernel32.GetCurrentProcess.restype = wintypes.HANDLE
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel32.CloseHandle.restype = wintypes.BOOL

    handle = kernel32.CreateJobObjectW(None, f"Local\\QuantMaster.App.{os.getpid()}")
    if not handle:
        raise OSError(ctypes.get_last_error(), "CreateJobObjectW failed")  # type: ignore[attr-defined]
    try:
        limits = _ExtendedLimitInformation()
        limits.BasicLimitInformation.LimitFlags = (
            0x00002000  # JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
            | 0x00000400  # JOB_OBJECT_LIMIT_DIE_ON_UNHANDLED_EXCEPTION
        )
        if not kernel32.SetInformationJobObject(
            handle,
            9,
            ctypes.byref(limits),
            ctypes.sizeof(limits),
        ):
            raise OSError(
                ctypes.get_last_error(),  # type: ignore[attr-defined]
                "SetInformationJobObject failed",
            )
        if not kernel32.AssignProcessToJobObject(handle, kernel32.GetCurrentProcess()):
            raise OSError(
                ctypes.get_last_error(),  # type: ignore[attr-defined]
                "AssignProcessToJobObject failed",
            )
        return handle
    except BaseException:
        kernel32.CloseHandle(handle)
        raise


def initialize_windows_app_process(*, root: bool = False) -> bool:
    """Apply one app identity and optionally own the inherited process tree.

    Child processes inherit membership in the root Job Object automatically.
    They still call this function to receive the same AppUserModelID.  Failure
    is non-fatal because shells, CI runners and endpoint security can place the
    launcher in a Job hierarchy that rejects another nested assignment.
    """

    global _ROOT_JOB
    if os.name != "nt":
        return False
    try:
        _set_app_user_model_id()
    except OSError:
        logger.warning("QuantMaster Windows 应用身份设置失败", exc_info=True)
    if not root or os.environ.get(APP_JOB_ENV) or _ROOT_JOB is not None:
        return _ROOT_JOB is not None or bool(os.environ.get(APP_JOB_ENV))
    try:
        _ROOT_JOB = _create_root_job()
    except OSError:
        logger.warning("QuantMaster Windows 根进程组创建失败", exc_info=True)
        return False
    os.environ[APP_JOB_ENV] = str(os.getpid())
    return True


def _role_executable(role: str) -> str | None:
    """Return a role-labelled interpreter copy for Windows Task Manager.

    ``multiprocessing.Process.name`` is only Python metadata: Windows displays
    the executable image name.  A sibling copy of the current interpreter
    preserves normal spawn semantics while making the process tree useful to a
    human (for example ``QuantMaster Runtime Worker.exe``).
    """

    if os.name != "nt":
        return None
    # ``sys.executable`` is normally the venv redirector (a tiny executable
    # with no adjacent CPython DLLs). Its renamed copies cannot start outside
    # the original ``python.exe`` name. A base-interpreter copy alongside
    # ``pyvenv.cfg`` retains the venv while giving Task Manager a role image.
    source = Path(getattr(sys, "_base_executable", sys.executable)).resolve()
    target_directory = Path(sys.executable).resolve().parent
    if not source.is_file() or source.suffix.casefold() != ".exe":
        return None
    safe_role = "".join(
        character for character in role if character.isalnum() or character in " -_"
    ).strip()
    if not safe_role:
        return None
    target = target_directory / f"QuantMaster {safe_role}.exe"
    if target == source:
        return str(source)
    try:
        if not target.exists() or target.stat().st_size != source.stat().st_size:
            temporary = target.with_name(f".{target.name}.{os.getpid()}.tmp")
            try:
                shutil.copy2(source, temporary)
                os.replace(temporary, target)
            finally:
                temporary.unlink(missing_ok=True)
        # Windows resolves dependent DLLs before Python can restore a venv's
        # base directory.  A renamed venv interpreter must therefore retain
        # the two CPython runtime DLLs beside itself.
        base_directory = source.parent
        for name in ("python312.dll", "python3.dll"):
            dependency = base_directory / name
            destination = target.with_name(name)
            if dependency.is_file() and (
                not destination.exists()
                or destination.stat().st_size != dependency.stat().st_size
            ):
                shutil.copy2(dependency, destination)
        return str(target)
    except OSError:
        logger.warning("无法准备 %s 的 Windows 角色启动器", role, exc_info=True)
        return None


def start_windows_role_process(process: Any, role: str) -> None:
    """Start a multiprocessing child with an observable Windows role name.

    The spawn executable is process-global in CPython, so serialise the small
    prepare/start/restore critical section.  The original executable is always
    restored before application code can create an unrelated child.
    """

    # Tests and embedded hosts can use Windows spawn without the application
    # root Job Object.  Do not alter their interpreter contract; role-labelled
    # images are strictly an owned QuantMaster application-tree feature.
    if os.name != "nt" or not os.environ.get(APP_JOB_ENV):
        process.start()
        return
    with _ROLE_EXECUTABLE_LOCK:
        executable = _role_executable(role)
        if not executable:
            process.start()
            return
        previous = spawn.get_executable()
        multiprocessing.set_executable(executable)
        try:
            process.start()
        finally:
            multiprocessing.set_executable(os.fsdecode(previous))
