"""Repository and local-data diagnostics used by ``qm doctor --deep``."""

from __future__ import annotations

import ast
import ipaddress
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from quantmaster.config import get_config
from quantmaster.release import RELEASE_DATE, VERSION
from quantmaster.runtime.sqlite import connect_sqlite

PACKAGE_ROOT = Path(__file__).resolve().parent


def _issue(
    code: str,
    severity: str,
    title: str,
    detail: str,
    *,
    target: str = "",
) -> dict[str, Any]:
    return {
        "code": code,
        "severity": severity,
        "title": title,
        "detail": detail[:2000],
        "target": target,
    }


def _module(path: Path) -> str:
    return ".".join(path.relative_to(PACKAGE_ROOT.parent).with_suffix("").parts)


def _imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    values: set[str] = set()
    for node in tree.body:
        if isinstance(node, ast.ImportFrom) and node.module:
            values.add(node.module)
        elif isinstance(node, ast.Import):
            values.update(alias.name for alias in node.names)
    return values


def _architecture_issues() -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    paths = [path for path in PACKAGE_ROOT.rglob("*.py") if path.name != "__init__.py"]
    modules = {_module(path): path for path in paths}
    graph = {
        module: {name for name in _imports(path) if name in modules}
        for module, path in modules.items()
    }
    visited: set[str] = set()
    active: set[str] = set()
    stack: list[str] = []
    cycles: set[tuple[str, ...]] = set()

    def visit(module: str) -> None:
        visited.add(module)
        active.add(module)
        stack.append(module)
        for dependency in graph[module]:
            if dependency not in visited:
                visit(dependency)
            elif dependency in active:
                cycles.add(tuple([*stack[stack.index(dependency):], dependency]))
        stack.pop()
        active.remove(module)

    for module in graph:
        if module not in visited:
            visit(module)
    for cycle in sorted(cycles):
        issues.append(_issue(
            "import_cycle", "high", "包内存在顶层循环依赖", " -> ".join(cycle),
        ))

    allowed_sqlite = {"runtime/sqlite.py", "data/migration.py"}
    for path in paths:
        relative = path.relative_to(PACKAGE_ROOT).as_posix()
        imports = _imports(path)
        if relative != "cli.py" and not relative.startswith("server/"):
            leaked = sorted(
                name for name in imports
                if name == "quantmaster.server" or name.startswith("quantmaster.server.")
            )
            for name in leaked:
                issues.append(_issue(
                    "transport_dependency", "high", "领域模块依赖 Web 传输层",
                    name, target=relative,
                ))
        if relative in allowed_sqlite:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and isinstance(node.func.value, ast.Name)
                and node.func.value.id == "sqlite3"
                and node.func.attr == "connect"
            ):
                issues.append(_issue(
                    "sqlite_factory_bypass", "high", "生产代码绕过统一 SQLite 工厂",
                    f"line {node.lineno}", target=relative,
                ))
    return issues


def _sqlite_issues(root: Path) -> tuple[list[dict[str, Any]], int]:
    issues: list[dict[str, Any]] = []
    checked = 0
    for path in sorted(root.rglob("*")):
        if not path.is_file() or path.suffix.lower() not in {".sqlite", ".sqlite3", ".db"}:
            continue
        checked += 1
        try:
            with connect_sqlite(path, row_factory=True) as connection:
                result = connection.execute("PRAGMA quick_check").fetchall()
            messages = [str(row[0]) for row in result]
            if messages != ["ok"]:
                issues.append(_issue(
                    "sqlite_corrupt", "high", "SQLite 完整性检查失败",
                    "; ".join(messages), target=str(path),
                ))
        except Exception as exc:
            issues.append(_issue(
                "sqlite_unreadable", "high", "SQLite 无法打开或检查",
                f"{type(exc).__name__}: {exc}", target=str(path),
            ))
    return issues, checked


def _storage_issues(root: Path) -> tuple[list[dict[str, Any]], dict[str, int]]:
    issues: list[dict[str, Any]] = []
    counts = {"bars": 0, "bar_failures": 0, "research_partitions": 0,
              "research_failures": 0}
    bars_root = root / "bars"
    if bars_root.is_dir():
        from quantmaster.data.storage import BarStore

        store = BarStore(bars_root)
        for symbol in store.symbols():
            counts["bars"] += 1
            result = store.read(symbol, enqueue_repair=False)
            if result.status != "ready":
                counts["bar_failures"] += 1
                issues.append(_issue(
                    "bar_integrity", "warning", "可重建行情缓存异常",
                    f"{result.status}: {result.reason}", target=symbol,
                ))
    lake_root = root / "research_lake"
    if lake_root.is_dir():
        from quantmaster.research.lake import ResearchDataIntegrityError, ResearchLake

        lake = ResearchLake(lake_root)
        for metadata in lake.catalog.partitions():
            counts["research_partitions"] += 1
            try:
                lake.validate_partition(metadata, enqueue_repair=False)
            except ResearchDataIntegrityError as exc:
                counts["research_failures"] += 1
                issues.append(_issue(
                    "research_partition_integrity", "warning", "可重建研究分区异常",
                    str(exc), target=str(metadata.get("partition_key")),
                ))
    return issues, counts


def _persistent_work_issues() -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    from quantmaster.data.repair import get_data_repair_manager
    from quantmaster.lab.store import LabStore

    repairs = get_data_repair_manager().list(limit=1000)
    failed = [item for item in repairs if item["status"] == "failed"]
    pending = [item for item in repairs if item["status"] in {"queued", "running"}]
    if failed:
        issues.append(_issue(
            "repair_failed", "warning", "存在耗尽重试的数据修复任务",
            f"{len(failed)} 个任务需要人工检查",
        ))
    if pending:
        issues.append(_issue(
            "repair_pending", "info", "存在待执行的数据修复任务",
            f"{len(pending)} 个任务将按额度与退避策略执行",
        ))
    publications = LabStore().pending_publications(1000, due_only=False)
    if publications:
        issues.append(_issue(
            "lab_publication_pending", "warning", "模型数据发布尚未完成",
            f"{len(publications)} 个 outbox 项将在 Lab worker 中重试",
        ))
    return issues


def _api_issues() -> list[dict[str, Any]]:
    from quantmaster.server.app import app

    paths = _route_paths(app.routes)
    issues: list[dict[str, Any]] = []
    required = {
        "/api/v1/session", "/api/v1/health/live", "/api/v1/health/ready",
        "/api/v1/diagnostics", "/api/v1/jobs",
    }
    missing = sorted(required - paths)
    if missing:
        issues.append(_issue(
            "api_contract_missing", "high", "固定公共 API 契约缺失", ", ".join(missing),
        ))
    old = sorted(path for path in paths if path.startswith("/api/") and not path.startswith("/api/v1/"))
    if old:
        issues.append(_issue(
            "legacy_api_present", "high", "旧版 API 路径仍可路由", ", ".join(old[:30]),
        ))
    if "/api/v1/health" in paths:
        issues.append(_issue(
            "aggregate_health_present", "high", "聚合健康接口重新进入公共契约",
            "/api/v1/health 必须保持 404；请使用 /live、/ready 或 /diagnostics",
        ))
    return issues


def _route_paths(
    routes: Any,
    *,
    prefix: str = "",
    seen: set[int] | None = None,
) -> set[str]:
    """Flatten Starlette routes and FastAPI 0.141+ lazy included routers."""
    visited = seen if seen is not None else set()
    paths: set[str] = set()
    for route in routes:
        identity = id(route)
        if identity in visited:
            continue
        visited.add(identity)
        path = getattr(route, "path", None)
        if path is not None:
            paths.add(f"{prefix}{path}")
        original_router = getattr(route, "original_router", None)
        nested_routes = getattr(original_router, "routes", None)
        if nested_routes is None:
            continue
        context = getattr(route, "include_context", None)
        include_prefix = str(getattr(context, "prefix", "") or "")
        paths.update(_route_paths(
            nested_routes,
            prefix=f"{prefix}{include_prefix}",
            seen=visited,
        ))
    return paths


def run_doctor(*, deep: bool = False) -> dict[str, Any]:
    cfg = get_config()
    root = cfg.data_root.resolve()
    issues: list[dict[str, Any]] = []
    try:
        loopback = ipaddress.ip_address(cfg.server.host).is_loopback
    except ValueError:
        loopback = cfg.server.host.lower() == "localhost"
    if not loopback:
        issues.append(_issue(
            "non_loopback_host", "high", "Web 服务配置不是回环地址",
            str(cfg.server.host),
        ))
    if root.is_symlink():
        issues.append(_issue(
            "symlink_data_root", "warning", "数据根目录是符号链接",
            "迁移和隔离操作需要额外核对真实路径", target=str(root),
        ))
    usage = shutil.disk_usage(root)
    if usage.free < 1024**3:
        issues.append(_issue(
            "low_disk_space", "warning", "数据盘剩余空间不足 1 GiB",
            f"free={usage.free}", target=str(root),
        ))

    metrics: dict[str, Any] = {"sqlite_checked": 0}
    if deep:
        # Initialize/read durable queues and API components first so their databases are
        # included in the same integrity sweep rather than appearing after it.
        issues.extend(_persistent_work_issues())
        issues.extend(_architecture_issues())
        issues.extend(_api_issues())
        storage_issues, storage_counts = _storage_issues(root)
        issues.extend(storage_issues)
        metrics.update(storage_counts)
        sqlite_issues, checked = _sqlite_issues(root)
        issues.extend(sqlite_issues)
        metrics["sqlite_checked"] = checked
    counts = {
        level: sum(item["severity"] == level for item in issues)
        for level in ("high", "warning", "info")
    }
    status = "high_risk" if counts["high"] else "warning" if counts["warning"] else "ok"
    return {
        "status": status,
        "deep": deep,
        "version": VERSION,
        "release_date": RELEASE_DATE,
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "data_root": str(root),
        "counts": counts,
        "metrics": metrics,
        "issues": issues,
    }
