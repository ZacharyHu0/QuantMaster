"""Windows application identity and root process-tree ownership."""

from __future__ import annotations

import ctypes
import logging
import multiprocessing
import os
import shutil
import sys
import threading
import unicodedata
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
            | 0x00000800  # JOB_OBJECT_LIMIT_BREAKAWAY_OK
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


def terminate_root_job(root_pid: int) -> None:
    """Terminate one QuantMaster root Job Object from the activation helper."""

    if os.name != "nt":
        return
    try:
        pid = int(root_pid)
    except (TypeError, ValueError) as exc:
        raise ValueError("root Job Object PID 无效") from exc
    if pid <= 0:
        raise ValueError("root Job Object PID 无效")
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)  # type: ignore[attr-defined]
    kernel32.OpenJobObjectW.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.LPCWSTR]
    kernel32.OpenJobObjectW.restype = wintypes.HANDLE
    kernel32.TerminateJobObject.argtypes = [wintypes.HANDLE, wintypes.UINT]
    kernel32.TerminateJobObject.restype = wintypes.BOOL
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel32.CloseHandle.restype = wintypes.BOOL
    handle = kernel32.OpenJobObjectW(0x0008 | 0x0004, False, f"Local\\QuantMaster.App.{pid}")
    if not handle:
        raise OSError(ctypes.get_last_error(), "OpenJobObjectW failed")  # type: ignore[attr-defined]
    try:
        if not kernel32.TerminateJobObject(handle, 1):
            raise OSError(ctypes.get_last_error(), "TerminateJobObject failed")  # type: ignore[attr-defined]
    finally:
        kernel32.CloseHandle(handle)


def _safe_role(role: str) -> str:
    normalized = unicodedata.normalize("NFKC", str(role))
    safe = "".join(
        character if character.isalnum() or character in " .-_" else " "
        for character in normalized
    )
    return " ".join(safe.split()).strip(" .")


def _base_interpreter() -> Path | None:
    """Resolve a real CPython host even from an already-renamed child.

    CPython derives ``_base_executable`` by retaining the current image name.
    For ``QuantMaster Runtime Worker.exe`` that points at a same-named file in
    ``base_prefix`` which does not exist.  Falling back to the canonical base
    ``python.exe`` keeps grandchildren role-labelled too.
    """

    candidates = (
        Path(getattr(sys, "_base_executable", sys.executable)),
        Path(sys.base_prefix) / "python.exe",
        Path(sys.executable),
    )
    return next((path.resolve() for path in candidates if path.is_file()), None)


def _read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except OSError:
        return ""


def _role_executable(role: str) -> str | None:
    """Return a role-labelled interpreter copy for Windows Task Manager.

    ``multiprocessing.Process.name`` is only Python metadata: Windows displays
    the executable image name.  A sibling copy of the current interpreter
    preserves normal spawn semantics while making the process tree useful to a
    human (for example ``QuantMaster Runtime Worker.exe``).
    """

    if os.name != "nt":
        return None
    if getattr(sys, "frozen", False):
        # Copying a one-file archive writes another ~170 MiB and makes its
        # first multiprocessing import race antivirus/archive extraction.
        # The original frozen executable already has the exact spawn contract.
        return os.path.abspath(sys.executable)
    # ``sys.executable`` is normally the venv redirector (a tiny executable
    # with no adjacent CPython DLLs). Its renamed copies cannot start outside
    # the original ``python.exe`` name. A base-interpreter copy alongside
    # ``pyvenv.cfg`` retains the venv while giving Task Manager a role image.
    source = _base_interpreter()
    target_directory = Path(sys.executable).resolve().parent
    if source is None or source.suffix.casefold() != ".exe":
        return None
    safe_role = _safe_role(role)
    if not safe_role:
        return None
    target = target_directory / f"QuantMaster {safe_role}.exe"
    if target == source:
        return str(source)
    try:
        marker = target.with_suffix(target.suffix + ".role")
        from quantmaster.release import VERSION

        identity = f"{VERSION}|{safe_role}|{source.stat().st_mtime_ns}|{source.stat().st_size}"
        if not target.exists() or _read_text(marker) != identity:
            temporary = target.with_name(f".{target.name}.{os.getpid()}.tmp")
            try:
                shutil.copy2(source, temporary)
                # A one-file PyInstaller archive lives in the executable
                # overlay. Source-mode CPython copies can safely receive role
                # VERSIONINFO; packaged copies retain their original resource
                # while their image filename still exposes the worker role.
                if not getattr(sys, "frozen", False):
                    from quantmaster.runtime.windows_executable import (
                        write_version_resource,
                    )

                    write_version_resource(
                        temporary,
                        VERSION,
                        description=f"QuantMaster {safe_role}",
                        internal_name=f"QuantMaster {safe_role}",
                        original_filename=target.name,
                    )
                os.replace(temporary, target)
                marker.write_text(identity, encoding="utf-8")
            finally:
                temporary.unlink(missing_ok=True)
        # Windows resolves dependent DLLs before Python can restore a venv's
        # base directory.  A renamed venv interpreter must therefore retain
        # the two CPython runtime DLLs beside itself.
        base_directory = source.parent
        for name in (
            f"python{sys.version_info.major}{sys.version_info.minor}.dll",
            f"python{sys.version_info.major}.dll",
            "vcruntime140.dll",
            "vcruntime140_1.dll",
        ):
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
        previous_reset = os.environ.get("PYINSTALLER_RESET_ENVIRONMENT")
        reset_onefile = (
            getattr(sys, "frozen", False)
            and str(role).strip().casefold() == "compute worker"
        )
        multiprocessing.set_executable(executable)
        try:
            # A managed StockDB restart can leave the parent onefile overlay
            # with a native DLL lifetime that a fresh compute child cannot
            # initialise (0xC0000142).  Give only compute children a private
            # PyInstaller extraction; the parent keeps its own environment.
            if reset_onefile:
                os.environ["PYINSTALLER_RESET_ENVIRONMENT"] = "1"
            process.start()
        finally:
            if reset_onefile:
                if previous_reset is None:
                    os.environ.pop("PYINSTALLER_RESET_ENVIRONMENT", None)
                else:
                    os.environ["PYINSTALLER_RESET_ENVIRONMENT"] = previous_reset
            multiprocessing.set_executable(os.fsdecode(previous))
