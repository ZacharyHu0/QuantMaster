"""Chromium 管理主流程；只在显式浏览器 lane 执行。"""

from __future__ import annotations

import asyncio
import json
import multiprocessing
import os
import re
import signal
import socket
import subprocess
import sys
import threading
import time
import traceback
from pathlib import Path
from urllib.parse import unquote, urlsplit

import httpx
import pytest
import uvicorn

from quantmaster import __version__
from quantmaster.config import Config, set_config
from quantmaster.release import RELEASE_DATE
from quantmaster.settings import ConfigManager

pytestmark = [
    pytest.mark.skipif(
        os.environ.get("QM_RUN_UI") != "1",
        reason="浏览器验收使用独立 lane；设置 QM_RUN_UI=1 可显式启用",
    ),
    pytest.mark.timeout(300),
    pytest.mark.module_isolated_config,
]
playwright_sync = pytest.importorskip("playwright.sync_api")
STATIC_ROOT = Path(__file__).resolve().parents[1] / "quantmaster" / "server" / "static"


def _static_request_path(request_url: str) -> tuple[str, Path] | None:
    path = unquote(urlsplit(request_url).path)
    if not path.startswith("/static/"):
        return None
    relative = path.removeprefix("/static/")
    resource = (STATIC_ROOT / relative).resolve()
    resource.relative_to(STATIC_ROOT.resolve())
    assert resource.is_file(), f"Chromium requested an unattributed static resource: {path}"
    return path, resource


def _measure_workspace_resource_budgets(url: str, browser_workdir: Path) -> dict[str, object]:
    with httpx.Client(trust_env=False) as client:
        response = client.get(url)
        response.raise_for_status()
        html_bytes = len(response.content)
        shell_paths = {
            urlsplit(value).path
            for value in re.findall(r'(?:src|href)="([^"?]+)', response.text)
            if urlsplit(value).path.startswith("/static/")
        }

    with playwright_sync.sync_playwright() as manager:
        previous_cwd = Path.cwd()
        try:
            os.chdir(browser_workdir)
            browser = manager.chromium.launch()
        finally:
            os.chdir(previous_cwd)
        discovery = browser.new_page(viewport={"width": 1280, "height": 900})
        discovery.goto(url)
        discovery.wait_for_url(re.compile(r"#today/market(?:\?.*)?$"))
        targets = discovery.locator(
            "[data-workspace-pages] [data-workspace-page]"
        ).evaluate_all(
            """controls => controls.map(control => ({
              workspace: control.closest('[data-workspace-pages]').dataset.workspacePages,
              page: control.dataset.workspacePage,
            }))"""
        )
        targets.extend(discovery.locator(
            "header [data-tab]:not([data-workspace-page])"
        ).evaluate_all(
            "controls => controls.map(control => ({workspace:'runtime', page:control.dataset.tab}))"
        ))
        discovery.close()
        targets = sorted({(target["workspace"], target["page"]) for target in targets})

        views: dict[str, dict[str, object]] = {}
        union_resources: dict[str, dict[str, int]] = {}
        initial_static: dict[str, int] = {}
        initial_static_bytes = 0
        echarts_vendor_bytes = 0
        for workspace, page_name in targets:
            context = browser.new_context(viewport={"width": 1280, "height": 900})
            page = context.new_page()
            requested: list[tuple[str, str]] = []
            errors: list[str] = []
            page.on(
                "request",
                lambda request, items=requested: items.append(
                    (request.url, request.resource_type)
                ),
            )
            page.on("pageerror", lambda error, items=errors: items.append(str(error)))
            page.add_init_script(
                """window.__qmBudgetMounted = [];
                document.addEventListener('quantmaster:workspace-mounted', event => {
                  window.__qmBudgetMounted.push(`${event.detail.workspace}/${event.detail.page}`);
                });"""
            )
            route = f"{workspace}/{page_name}"
            page.goto(f"{url}/#{route}")
            page.wait_for_function(
                "route => window.__qmBudgetMounted.includes(route)", arg=route,
            )
            page.wait_for_load_state("networkidle")
            page.wait_for_timeout(250)
            page.wait_for_load_state("networkidle")
            assert errors == [], {route: errors}

            occurrences: list[tuple[str, Path, str]] = []
            for request_url, resource_type in requested:
                resource = _static_request_path(request_url)
                if resource is not None:
                    occurrences.append((*resource, resource_type))
            sizes = {path: resource.stat().st_size for path, resource, _ in occurrences}
            owned = [
                (path, resource, resource_type)
                for path, resource, resource_type in occurrences
                if path not in shell_paths and path != "/static/echarts.min.js"
            ]
            owned_sizes = {path: resource.stat().st_size for path, resource, _ in owned}
            owned_bytes = sum(resource.stat().st_size for _, resource, _ in owned)
            views[f"#{route}"] = {
                "owned_bytes": owned_bytes,
                "request_count": len(owned),
                "resources": owned_sizes,
            }
            union_resources.setdefault(workspace, {}).update(owned_sizes)

            if "/static/echarts.min.js" in sizes:
                echarts_vendor_bytes = sizes["/static/echarts.min.js"]
            if route == "today/market":
                initial_static_bytes = sum(
                    resource.stat().st_size for _, resource, resource_type in occurrences
                    if resource_type in {"script", "stylesheet"}
                    and resource.name != "echarts.min.js"
                )
                initial_static = {
                    path: resource.stat().st_size
                    for path, resource, resource_type in occurrences
                    if resource_type in {"script", "stylesheet"}
                }
            context.close()
        browser.close()

    unions = {
        workspace: {
            "owned_bytes": sum(resources.values()),
            "resources": dict(sorted(resources.items())),
        }
        for workspace, resources in sorted(union_resources.items())
    }
    return {
        "initial_raw_bytes": html_bytes + initial_static_bytes,
        "initial_resources": dict(sorted(initial_static.items())),
        "views": dict(sorted(views.items())),
        "workspace_unions": unions,
        "echarts_vendor_bytes": echarts_vendor_bytes,
    }


def _stop_live_server(process: subprocess.Popen, *, timeout: float = 20) -> None:
    """Let application lifespan release child processes before hard termination."""
    if process.poll() is not None:
        return
    graceful_signal = signal.CTRL_BREAK_EVENT if os.name == "nt" else signal.SIGINT
    try:
        process.send_signal(graceful_signal)
        process.wait(timeout=timeout)
        return
    except (OSError, subprocess.TimeoutExpired):
        pass
    process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=10)


def _wait_for_text(locator, text: str, *, timeout: float = 30_000) -> None:
    playwright_sync.expect(locator).to_contain_text(text, timeout=timeout)


def _wait_for_class(locator, class_name: str, *, timeout: float = 30_000) -> None:
    playwright_sync.expect(locator).to_have_class(
        re.compile(rf"(?:^|\s){re.escape(class_name)}(?:\s|$)"),
        timeout=timeout,
    )


def _wait_for_document_fit(page, *, timeout: float = 30_000) -> None:
    deadline = time.monotonic() + timeout / 1000
    dimensions = {"scrollWidth": -1, "innerWidth": -1}
    while time.monotonic() < deadline:
        dimensions = page.evaluate(
            "({scrollWidth: document.documentElement.scrollWidth, innerWidth: window.innerWidth})"
        )
        if dimensions["scrollWidth"] <= dimensions["innerWidth"]:
            return
        page.wait_for_timeout(50)
    raise AssertionError(f"页面存在横向溢出: {dimensions}")


def _active_chart_count(page) -> int:
    return page.evaluate(
        "Object.values(window.charts || {}).filter(chart => !chart.isDisposed()).length"
    )


def _wait_for_ui_health(url, thread, failures) -> None:
    deadline = time.monotonic() + 300
    with httpx.Client(trust_env=False) as client:
        while time.monotonic() < deadline and thread.is_alive() and not failures:
            try:
                if client.get(f"{url}/api/v1/health", timeout=0.3).status_code == 200:
                    return
            except httpx.HTTPError:
                time.sleep(0.1)
    raise RuntimeError(f"测试服务启动失败: {failures!r}")


def _wait_for_ui_runtime(url) -> None:
    from quantmaster.runtime.worker import runtime_worker_status

    deadline = time.monotonic() + 60
    readiness: dict[str, object] = {}
    with httpx.Client(trust_env=False, timeout=1) as client:
        while time.monotonic() < deadline:
            statuses = {}
            allowed_degraded = set()
            for path in ("/api/v1/market/overview", "/api/v1/backtests", "/api/v1/paper/accounts"):
                try:
                    response = client.get(f"{url}{path}")
                    statuses[path] = response.status_code
                    if path == "/api/v1/market/overview" and response.status_code == 503:
                        try:
                            if response.json()["problem"]["code"] in {
                                "calendar_unavailable", "snapshot_unavailable",
                            }:
                                allowed_degraded.add(path)
                        except (KeyError, TypeError, ValueError):
                            pass
                except httpx.HTTPError:
                    statuses[path] = 599
            worker = runtime_worker_status()
            readiness = {"statuses": statuses, "worker": worker}
            if (
                worker.get("available")
                and worker.get("status") == "running"
                and int(worker.get("pid") or 0) == os.getpid()
                and all(
                    status < 500 or path in allowed_degraded
                    for path, status in statuses.items()
                )
            ):
                return
            time.sleep(0.1)
    raise RuntimeError(f"UI 测试运行时未就绪: {readiness!r}")


def _assert_no_ui_process_owners() -> None:
    deadline = time.monotonic() + 1
    while True:
        blocking_threads = [
            active for active in threading.enumerate()
            if active is not threading.main_thread()
            and active.is_alive()
            and not active.daemon
            and not active.name.startswith("pytest_timeout ")
        ]
        children = multiprocessing.active_children()
        if not blocking_threads and not children:
            return
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        time.sleep(min(0.01, remaining))

    frames = sys._current_frames()
    threads = []
    for active in threading.enumerate():
        if active is threading.main_thread() or not active.is_alive():
            continue
        frame = frames.get(active.ident) if active.ident is not None else None
        threads.append({
            "name": active.name, "ident": active.ident, "daemon": active.daemon,
            "stack": "".join(traceback.format_stack(frame)) if frame else "unavailable",
        })
    children = [
        {"pid": child.pid, "name": child.name, "exitcode": child.exitcode}
        for child in multiprocessing.active_children()
    ]
    blocking_threads = [
        item for item in threads
        if not item["daemon"] and not str(item["name"]).startswith("pytest_timeout ")
    ]
    assert not blocking_threads and not children, (
        f"UI 测试生命周期仍有阻塞所有者: threads={blocking_threads!r} "
        f"children={children!r} daemon_threads={threads!r}"
    )


def test_ui_owner_assertion_waits_for_short_lived_runtime_thread() -> None:
    started = threading.Event()

    def finish_soon() -> None:
        started.set()
        time.sleep(0.05)

    owner = threading.Thread(target=finish_soon, name="short-lived-ui-owner")
    owner.start()
    assert started.wait(timeout=1)
    try:
        _assert_no_ui_process_owners()
        assert not owner.is_alive()
    finally:
        owner.join(timeout=1)


def test_after_close_prioritizes_sector_width_without_wide_screen_scroll(live_server):
    url, _ = live_server
    with playwright_sync.sync_playwright() as manager:
        browser = manager.chromium.launch()
        page = browser.new_page(viewport={"width": 1280, "height": 900})
        page.goto(f"{url}/#today/after-close")
        workbench = page.locator(".after-close-workbench")
        workbench.wait_for(state="visible")

        sector = page.locator(".after-close-sector-pane")
        candidate = page.locator(".after-close-candidate-pane")
        assert sector.bounding_box()["width"] > candidate.bounding_box()["width"]
        assert sector.locator(".after-close-table-wrap").evaluate(
            "node => node.scrollWidth <= node.clientWidth"
        )
        _wait_for_document_fit(page)

        page.set_viewport_size({"width": 1100, "height": 900})
        assert workbench.evaluate(
            "node => getComputedStyle(node).gridTemplateColumns.split(' ').length"
        ) == 1
        _wait_for_document_fit(page)
        browser.close()


@pytest.fixture(scope="module")
def module_config(tmp_path_factory, _minimal_security_master):
    root = tmp_path_factory.mktemp("qm-ui")
    cfg = Config()
    cfg.data.root = str(root / "data")
    cfg.data_root.mkdir(parents=True, exist_ok=True)
    (cfg.data_root / "security_master.sqlite").write_bytes(
        Path(_minimal_security_master).read_bytes()
    )
    previous = {name: os.environ.get(name) for name in (
        "QM_DATA_ROOT", "QM_CONFIG_PATH", "QM_FREE_STOCKDB_MANAGED",
        "QM_DISABLE_WORKER_SUPERVISOR",
    )}
    os.environ["QM_DATA_ROOT"] = str(cfg.data_root)
    os.environ["QM_CONFIG_PATH"] = str(root / "config.yaml")
    os.environ["QM_FREE_STOCKDB_MANAGED"] = "false"
    os.environ["QM_DISABLE_WORKER_SUPERVISOR"] = "1"
    set_config(cfg)
    from quantmaster.data.migration import migration_manager

    previous_manager = migration_manager.config_manager
    task_manager = ConfigManager(root / "config.yaml", root / "config.snapshots")
    migration_manager.config_manager = task_manager
    management_module = sys.modules.get("quantmaster.server.management")
    if management_module is not None:
        management_module.settings_manager = task_manager
    assert migration_manager.config_manager is task_manager
    assert task_manager.backup_dir == (root / "config.snapshots").resolve()
    from quantmaster.ai.crawler import NewsStore

    assert NewsStore().recent(limit=1) == []
    assert NewsStore(read_only=True).recent(limit=1) == []
    try:
        yield cfg, root
    finally:
        management_module = sys.modules.get("quantmaster.server.management")
        if management_module is not None:
            management_module.settings_manager = previous_manager
        migration_manager.config_manager = previous_manager
        set_config(None)
        for name, value in previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


@pytest.fixture(scope="module")
def live_server(module_config):
    cfg, root = module_config
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    from quantmaster.data.migration import migration_manager
    from quantmaster.server import management
    from quantmaster.server.app import app as ui_test_app
    from quantmaster.server.app import create_lifespan

    assert management.settings_manager is migration_manager.config_manager
    assert management.settings_manager.backup_dir == (root / "config.snapshots").resolve()
    production_lifespan = ui_test_app.router.lifespan_context
    ui_test_app.router.lifespan_context = create_lifespan(bootstrap_rotation=False)
    server = uvicorn.Server(uvicorn.Config(
        ui_test_app, host="127.0.0.1", port=port, log_level="warning",
        log_config=None, access_log=False, timeout_graceful_shutdown=10,
    ))
    failures: list[BaseException] = []
    loop_ready = threading.Event()
    server_loop: list[asyncio.AbstractEventLoop] = []

    def run_server() -> None:
        try:
            with asyncio.Runner(loop_factory=asyncio.new_event_loop) as runner:
                server_loop.append(runner.get_loop())
                loop_ready.set()
                runner.run(server.serve())
        except BaseException as exc:
            failures.append(exc)

    def request_server_stop() -> None:
        if not loop_ready.wait(timeout=5) or not server_loop:
            raise RuntimeError("UI 测试服务 event loop 未就绪")
        loop = server_loop[0]
        if loop.is_closed():
            if thread.is_alive():
                raise RuntimeError("UI 测试服务 event loop 已关闭但线程仍存活")
            return
        loop.call_soon_threadsafe(setattr, server, "should_exit", True)

    thread = threading.Thread(target=run_server, name="qm-ui-test-server")
    thread.start()
    url = f"http://127.0.0.1:{port}"
    try:
        _wait_for_ui_health(url, thread, failures)
    except RuntimeError:
        request_server_stop()
        thread.join(timeout=20)
        raise
    from quantmaster.server.bootstrap import get_runtime_worker

    owned_worker = get_runtime_worker()
    heartbeat_path = cfg.data_root / "runtime-worker.json"
    try:
        _wait_for_ui_runtime(url)
    except RuntimeError:
        request_server_stop()
        thread.join(timeout=20)
        raise
    try:
        yield url, root
    finally:
        os.environ["QM_DATA_ROOT"] = str(cfg.data_root)
        os.environ["QM_CONFIG_PATH"] = str(root / "config.yaml")
        os.environ["QM_DISABLE_WORKER_SUPERVISOR"] = "1"
        set_config(cfg)
        owned_worker.stop()
        assert owned_worker.status()["in_process_started"] is False
        assert not heartbeat_path.exists()
        request_server_stop()
        thread.join(timeout=60)
        ui_test_app.router.lifespan_context = production_lifespan
        if thread.is_alive():
            frame = sys._current_frames().get(thread.ident) if thread.ident is not None else None
            stack = "".join(traceback.format_stack(frame)) if frame else "unavailable"
            raise AssertionError(f"UI 测试服务未在期限内停止: {failures=!r} server_stack={stack}")
        assert not failures, f"UI 测试服务异常退出: {failures!r}"
        _assert_no_ui_process_owners()


def _install_market_workbench_routes(page, *, list_status: int = 200):
    methods = {
        name: {
            "status": "ready", "last": 1010 + index,
            "changes": {"1": 0.4 + index, "3": 1.0 + index, "5": 2.0 + index, "20": 4.0 + index},
            "sessions": 120, "reason": "",
        }
        for index, name in enumerate(("equal", "float_mv", "amount", "volume", "total_mv"))
    }
    boards = [
        {
            "code": "SW1:801780.SI", "board_code": "801780.SI", "name": "银行",
            "category": "sw1", "level": "L1", "member_count": 2,
            "eligible_count": 2, "coverage": 1.0,
        },
        {
            "code": "SW1:801150.SI", "board_code": "801150.SI", "name": "医药生物",
            "category": "sw1", "level": "L1", "member_count": 2,
            "eligible_count": 2, "coverage": 0.96,
        },
    ]
    meta = {
        "snapshot_id": "board-ui", "as_of": "2026-08-17",
        "algorithm_version": "QM_BOARD_INDEX_V1",
        "board_index_algorithm_version": "QM_BOARD_INDEX_V1",
        "quality": {"status": "complete", "issues": []},
        "sources": ["free-stockdb:boards", "free-stockdb:zhishu"],
    }

    page.route("**/api/v1/market/overview", lambda route: route.fulfill(json={
        "snapshot": {"id": "market-ui", "state": "fresh", "as_of": "2026-08-17"},
        "data": {
            "groups": {"A股指数": [
                {"symbol": "000001.SH", "name": "上证指数", "last": 3688.2, "change_pct": 0.72},
                {"symbol": "000300.SH", "name": "沪深300", "last": 4210.8, "change_pct": -0.18},
            ]},
            "data_quality": {"status": "verified", "observed_count": 22, "requested_count": 24},
            "meta": {"as_of": "2026-08-17"},
        },
    }))
    page.route("**/api/v1/market/fear-greed", lambda route: route.fulfill(json={
        "status": "stale", "score": 31, "rating_label": "恐惧",
    }))
    page.route("**/api/v1/market/ashare-fear-greed**", lambda route: route.fulfill(json={
        "status": "ready", "score": 57, "rating_label": "中性",
    }))
    page.route("**/api/v1/market/temperature", lambda route: route.fulfill(json={
        "meta": {"snapshot_id": "temp-ui", "as_of": "2026-08-17", "quality": {"status": "complete"}},
        "data": {"current": {"temperature": 63}},
    }))
    page.route("**/api/v1/rotation/overview**", lambda route: route.fulfill(json={
        "meta": {"snapshot_id": "rotation-ui", "as_of": "2026-08-17", "quality": {"status": "partial"}},
        "data": {"market": {"temperature": {"temperature": 63}}},
    }))

    def board_list(route):
        if list_status != 200:
            route.fulfill(status=list_status, json={"problem": {"message": "板块指数快照尚未发布"}})
            return
        query = urlsplit(route.request.url).query
        params = dict(item.split("=", 1) for item in query.split("&") if "=" in item)
        selected = boards
        if params.get("query"):
            selected = boards[:1]
        method = params.get("method", "equal")
        window = params.get("window", "5")
        items = [{**item, "method": method, "status": "ready", "last": methods[method]["last"],
                  "change": methods[method]["changes"][window], "changes": methods[method]["changes"],
                  "sessions": 120, "reason": ""} for item in selected]
        route.fulfill(json={"meta": meta, "data": {
            "items": items, "category": "sw1", "method": method, "window": int(window),
            "pagination": {"page": 1, "page_size": 25, "total": len(items), "pages": 1,
                           "has_previous": False, "has_next": False},
        }})

    page.route("**/api/v1/rotation/board-indexes?*", board_list)

    def detail(route):
        request_url = urlsplit(route.request.url)
        query = dict(
            item.split("=", 1) for item in request_url.query.split("&") if "=" in item
        )
        method = query.get("method", "equal")
        code = request_url.path.rsplit("/", 1)[-1]
        board = next((item for item in boards if item["board_code"] == code), boards[0])
        route.fulfill(json={"meta": meta, "data": {
            **board, "membership_semantics": "current_constituents_backcast",
            "frequency": "1d", "base": 1000, "method": method,
            "method_status": methods[method], "comparison": methods, "constituent_count": 2,
            "series": [{"date": f"2026-08-{day:02d}", "close": 1000 + day * 2 + list(methods).index(method)}
                       for day in range(1, 18)],
        }})

    page.route(re.compile(r".*/api/v1/rotation/board-indexes/(sw1|sw2|theme)/[^/?]+\?method=.*"), detail)
    page.route("**/api/v1/rotation/board-indexes/*/*/constituents?*", lambda route: route.fulfill(json={
        "meta": meta, "data": {
            "name": "银行", "membership_semantics": "current_constituents_backcast",
            "items": [
                {
                    "symbol": "600000.SH", "name": "浦发银行", "last": 12.4,
                    "change_pct": 1.2, "amount": 8e8, "as_of": "2026-08-17",
                },
                {
                    "symbol": "000001.SZ", "name": "平安银行", "last": 11.8,
                    "change_pct": -0.4, "amount": 7e8, "as_of": "2026-08-17",
                },
            ],
            "pagination": {"page": 1, "page_size": 25, "total": 2, "pages": 1,
                           "has_previous": False, "has_next": False},
        },
    }))
    page.route("**/api/v1/market/history/*", lambda route: route.fulfill(json={
        "symbol": "600000.SH", "frequency": "1d",
        "kline": [[f"2026-08-{day:02d}", 10 + day / 10, 10.1 + day / 10,
                   9.8 + day / 10, 10.3 + day / 10, 1000 + day] for day in range(1, 18)],
        "data_quality": {"status": "verified"}, "provenance": [],
    }))


def test_market_workbench_deep_link_algorithms_keyboard_and_budgets(live_server, tmp_path):
    url, _ = live_server
    with playwright_sync.sync_playwright() as manager:
        browser = manager.chromium.launch()
        page = browser.new_page(viewport={"width": 1440, "height": 900})
        page.add_init_script(
            """window.__marketAbortCount = 0;
            const NativeAbortController = window.AbortController;
            window.AbortController = class extends NativeAbortController {
              abort(...args) { window.__marketAbortCount += 1; return super.abort(...args); }
            };"""
        )
        errors: list[str] = []
        page.on("pageerror", lambda error: errors.append(str(error)))
        _install_market_workbench_routes(page)
        page.goto(f"{url}/#today/market?category=sw1&code=801780.SI&method=amount&window=20")
        page.locator("#market-board-chart canvas").wait_for(state="visible")

        assert "category=sw1" in page.url and "code=801780.SI" in page.url
        assert "method=amount" in page.url and "window=20" in page.url
        playwright_sync.expect(
            page.locator('[data-market-method="amount"]').first
        ).to_have_attribute("aria-selected", "true")
        assert page.locator("#market-method-compare > button").count() == 5
        assert page.locator(".market-decision-strip > :is(article,.market-decision-metric)").count() >= 6
        assert page.locator("#tab-market *").count() <= 2500
        assert _active_chart_count(page) <= 4
        focus = page.locator(".market-focus").bounding_box()
        assert focus and focus["y"] < 330 and focus["y"] + focus["height"] <= 900

        page.locator('[data-market-method="amount"]').first.focus()
        page.keyboard.press("ArrowRight")
        playwright_sync.expect(
            page.locator('[data-market-method="volume"]').first
        ).to_have_attribute("aria-selected", "true")
        page.wait_for_function("() => location.hash.includes('method=volume')")

        aborts_before = page.evaluate("window.__marketAbortCount")
        page.locator('[data-market-board="801150.SI"]').click()
        page.locator('[data-market-board="801780.SI"]').click()
        page.wait_for_function("value => window.__marketAbortCount > value", arg=aborts_before)
        page.locator("#market-board-chart canvas").wait_for(state="visible")

        page.locator('[data-market-category="sw1"]').focus()
        page.keyboard.press("ArrowRight")
        playwright_sync.expect(
            page.locator('[data-market-category="sw2"]')
        ).to_have_attribute("aria-selected", "true")
        page.locator('[data-market-category="sw1"]').click()
        page.locator("[data-market-query]").fill("银行")
        playwright_sync.expect(page.locator("[data-market-board]")).to_have_count(1)

        page.locator("#runtime-info").evaluate("node => { node.hidden = true; }")
        desktop_shot = tmp_path / "market-workbench-1440.png"
        page.screenshot(path=str(desktop_shot), full_page=True)
        assert desktop_shot.stat().st_size > 10_000

        page.set_viewport_size({"width": 3840, "height": 2160})
        page.locator("#market-board-chart canvas").wait_for(state="visible")
        workbench_box = page.locator(".market-workbench").bounding_box()
        focus_box = page.locator(".market-focus").bounding_box()
        chart_box = page.locator("#market-board-chart").bounding_box()
        assert workbench_box and workbench_box["width"] >= 3600
        assert focus_box and focus_box["width"] >= 2400
        assert chart_box and chart_box["width"] >= 2400

        page.set_viewport_size({"width": 390, "height": 844})
        _wait_for_document_fit(page)
        assert page.locator(".market-three-column").evaluate(
            "node => getComputedStyle(node).display"
        ) == "flex"
        assert page.locator("#tab-market *").count() <= 2500
        mobile_shot = tmp_path / "market-workbench-390.png"
        page.screenshot(path=str(mobile_shot), full_page=True)
        assert mobile_shot.stat().st_size > 10_000
        assert errors == []
        browser.close()


def test_market_workbench_on_demand_history_restored_detail_routes_and_failure(live_server):
    url, _ = live_server
    with playwright_sync.sync_playwright() as manager:
        browser = manager.chromium.launch()
        page = browser.new_page(viewport={"width": 1280, "height": 900}, reduced_motion="reduce")
        requests: list[str] = []
        page.on("request", lambda request: requests.append(request.url))
        _install_market_workbench_routes(page)
        page.goto(f"{url}/#today/market")
        page.locator("#market-board-chart canvas").wait_for(state="visible")
        assert not any("/market/history/" in value for value in requests)
        page.locator('[data-market-stock="600000.SH"]').click()
        page.locator("#market-stock-chart canvas").wait_for(state="visible")
        assert sum("/market/history/600000.SH" in value for value in requests) == 1
        assert page.locator(".market-loading-line i").evaluate_all(
            "nodes => nodes.every(node => getComputedStyle(node).animationName === 'none')"
        )

        page.get_by_role("button", name="账户", exact=True).click()
        page.wait_for_url(re.compile(r"#account/paper$"))
        assert page.evaluate(
            "Object.keys(window.charts || {}).filter(key => key.startsWith('market-')).length"
        ) == 0

        for detail, selector in (
            ("quotes", "#market-quotes-view"),
            ("temperature", "#market-temperature-view"),
            ("style", "#market-style-view"),
            ("rotation", "#rotation-overview-view"),
        ):
            page.goto(f"{url}/#today/{detail}")
            page.locator(selector).wait_for(state="visible")
            assert page.url.endswith(f"#today/{detail}")
            assert page.locator("#market-workbench-view").is_hidden()

        page.goto(f"{url}/#today/market")
        page.locator('a[href="#today/quotes?focus=ashare-fear-greed"]').click()
        page.locator("#market-ashare-fear-greed").wait_for(state="visible")
        assert "focus=ashare-fear-greed" in page.url
        page.goto(f"{url}/#today/market")
        page.locator('a[href="#today/quotes?focus=fear-greed"]').click()
        page.locator("#market-fear-greed").wait_for(state="visible")
        assert "focus=fear-greed" in page.url

        failed = browser.new_page(viewport={"width": 390, "height": 844})
        _install_market_workbench_routes(failed, list_status=503)
        failed.goto(f"{url}/#today/market")
        playwright_sync.expect(failed.locator("#market-board-list")).to_contain_text("板块指数快照尚未发布")
        _wait_for_document_fit(failed)
        browser.close()


def _legacy_today_uses_native_canvas_without_echarts_across_themes(live_server):
    url, _ = live_server
    market = {
        "groups": {"A股指数": [{
            "symbol": "000300.SH", "name": "沪深300", "last": 4600.0,
            "change_pct": -0.2, "nav": [[1784505600000, 1.0], [1784592000000, 0.998]],
            "as_of": "2026-07-21", "cache_status": "ready", "rsi_14": 60.0,
            "rsi_history": [["2026-05-01", 35.0], ["2026-07-21", 60.0]],
        }]},
    }
    fear_greed = {
        "status": "ready", "score": 18.0, "rating_label": "极度恐惧",
        "as_of": "2026-07-21T08:00:00+08:00",
        "history": [{"date": "2026-07-20", "score": 22.0}, {"date": "2026-07-21", "score": 18.0}],
        "thresholds": {"fear_greed_rare": 10, "rsi_add": 22},
    }

    with playwright_sync.sync_playwright() as manager:
        browser = manager.chromium.launch()
        page = browser.new_page(viewport={"width": 1280, "height": 900})
        page.add_init_script(
            """localStorage.setItem('qm-theme', 'ink');
            window.__marketIntersections = [];
            window.IntersectionObserver = class {
              constructor(callback) { this.callback = callback; this.targets = new Set(); }
              observe(target) { this.targets.add(target); window.__marketIntersections.push(this); }
              unobserve(target) { this.targets.delete(target); }
              disconnect() { this.targets.clear(); }
            };"""
        )
        requested = []
        errors = []
        page.on("request", lambda request: requested.append(request.url))
        page.on("pageerror", lambda error: errors.append(str(error)))
        page.route("**/api/v1/market/overview", lambda route: route.fulfill(json=market))
        page.route("**/api/v1/market/fear-greed", lambda route: route.fulfill(json=fear_greed))

        page.goto(url)
        page.wait_for_url(re.compile(r"#today/quotes$"))
        gauge = page.locator("#fear-greed-gauge-market canvas")
        history = page.locator("#fear-greed-history-market canvas")
        spark = page.locator('[data-market-group="A股指数"] .spark canvas')
        for canvas in (gauge, history, spark):
            canvas.wait_for(state="visible")
        assert "18.0，极度恐惧" in page.locator("#fear-greed-gauge-market").get_attribute("aria-label")
        assert not any(path.endswith(("/echarts.min.js", "/charts.js", "/charts.css")) for path in requested)
        assert page.locator(".market-sentiment-panel").evaluate_all(
            "nodes => nodes.every(node => "
            "getComputedStyle(node).gridTemplateColumns.split(' ').length === 1)"
        )
        assert page.locator(".app-header").bounding_box()["height"] <= 130
        assert page.locator(".header-help > span").evaluate(
            "node => getComputedStyle(node).display"
        ) == "none"

        spark.scroll_into_view_if_needed()
        bounds = spark.bounding_box()
        page.mouse.move(bounds["x"] + bounds["width"] * 0.75, bounds["y"] + bounds["height"] / 2)
        tooltip = page.locator('[data-market-group="A股指数"] .native-chart-tooltip')
        tooltip.wait_for(state="visible")
        assert "区间涨跌" in tooltip.inner_text()
        assert "当日涨跌" in tooltip.inner_text()

        page.set_viewport_size({"width": 390, "height": 844})
        _wait_for_document_fit(page)
        assert spark.bounding_box()["width"] <= 390
        assert page.locator(".app-header").bounding_box()["height"] <= 180
        assert page.locator(".workspace-context-label").evaluate_all(
            "nodes => nodes.every(node => getComputedStyle(node).display === 'none')"
        )
        assert errors == []

        classic = browser.new_page(viewport={"width": 1280, "height": 900})
        classic.add_init_script(
            """localStorage.setItem('qm-theme', 'classic');
            window.__marketIntersections = [];
            window.IntersectionObserver = class {
              constructor(callback) { this.callback = callback; this.targets = new Set(); }
              observe(target) { this.targets.add(target); window.__marketIntersections.push(this); }
              unobserve(target) { this.targets.delete(target); }
              disconnect() { this.targets.clear(); }
            };
            window.__showMarketSparks = () => window.__marketIntersections.forEach(observer => {
              const entries = [...observer.targets].map(target => ({target, isIntersecting:true}));
              if (entries.length) observer.callback(entries);
            });"""
        )
        classic.route("**/api/v1/market/overview", lambda route: route.fulfill(json=market))
        classic.route("**/api/v1/market/fear-greed", lambda route: route.fulfill(json=fear_greed))
        classic.goto(url)
        classic.wait_for_url(re.compile(r"#today/quotes$"))
        classic.locator('[data-market-group="A股指数"] .spark').wait_for(state="visible")
        assert classic.locator('[data-market-group="A股指数"] .spark canvas').count() == 0
        classic.evaluate("window.__showMarketSparks()")
        classic.locator('[data-market-group="A股指数"] .spark canvas').wait_for(state="visible")
        browser.close()


def test_classic_theme_is_default_without_overwriting_stored_choice(live_server):
    url, _ = live_server
    with playwright_sync.sync_playwright() as manager:
        browser = manager.chromium.launch(headless=True)
        cases = (
            ("", "classic"),
            ("localStorage.setItem('qm-theme', 'ink')", "ink"),
            ("""
              const getItem = Storage.prototype.getItem;
              Storage.prototype.getItem = function(key) {
                if (key !== 'qm-theme') return getItem.call(this, key);
                Storage.prototype.getItem = getItem;
                throw new Error('blocked');
              };
            """, "classic"),
        )
        for init_script, expected in cases:
            page = browser.new_page()
            errors: list[str] = []
            page.on("pageerror", lambda error, items=errors: items.append(str(error)))
            if init_script:
                page.add_init_script(init_script)
            page.goto(url)
            page.wait_for_load_state("networkidle")

            assert page.locator("html").get_attribute("data-qm-theme") == expected
            assert page.locator(f"#qm-theme-{expected}").is_checked()
            assert errors == []
            page.close()
        browser.close()


def _legacy_fear_greed_gauge_animates_normally_and_respects_reduced_motion(live_server):
    url, _ = live_server
    fear_greed = {
        "status": "ready", "score": 18.0, "rating_label": "极度恐惧",
        "history": [{"date": "2026-07-20", "score": 22.0}],
    }
    capture = r"""
      window.__gaugeValues = [];
      const fillText = CanvasRenderingContext2D.prototype.fillText;
      CanvasRenderingContext2D.prototype.fillText = function(text, ...args) {
        if (this.canvas.closest?.('#fear-greed-gauge-market') && /^\d+\.\d$/.test(String(text))) {
          window.__gaugeValues.push(String(text));
        }
        return fillText.call(this, text, ...args);
      };
    """

    with playwright_sync.sync_playwright() as manager:
        browser = manager.chromium.launch()
        normal = browser.new_page(viewport={"width": 1280, "height": 900})
        normal.add_init_script(capture)
        normal.route("**/api/v1/market/overview", lambda route: route.fulfill(json={"groups": {}}))
        normal.route("**/api/v1/market/fear-greed", lambda route: route.fulfill(json=fear_greed))
        normal.goto(url)
        normal.wait_for_function("() => window.__gaugeValues.includes('18.0')")
        normal_values = normal.evaluate("window.__gaugeValues")
        assert any(value != "18.0" for value in normal_values), normal_values

        reduced = browser.new_page(
            viewport={"width": 1280, "height": 900}, reduced_motion="reduce",
        )
        reduced.add_init_script(capture)
        reduced.route("**/api/v1/market/overview", lambda route: route.fulfill(json={"groups": {}}))
        reduced.route("**/api/v1/market/fear-greed", lambda route: route.fulfill(json=fear_greed))
        reduced.goto(url)
        reduced.wait_for_function("() => window.__gaugeValues.includes('18.0')")
        reduced.wait_for_timeout(700)
        assert set(reduced.evaluate("window.__gaugeValues")) == {"18.0"}
        browser.close()


def _legacy_today_unmount_cancels_native_chart_work_and_delayed_renders(live_server):
    url, _ = live_server
    market = {
        "groups": {"A股指数": [{
            "symbol": "000300.SH", "name": "沪深300", "last": 4600.0,
            "change_pct": -0.2, "nav": [[1784505600000, 1.0], [1784592000000, 0.998]],
            "as_of": "2026-07-21", "cache_status": "ready", "rsi_14": 60.0,
            "rsi_history": [["2026-05-01", 35.0], ["2026-07-21", 60.0]],
        }]},
    }
    fear_greed = {
        "status": "ready", "score": 18.0, "rating_label": "极度恐惧",
        "history": [{"date": "2026-07-20", "score": 22.0}],
    }
    lifecycle_script = """
      localStorage.setItem('qm-theme', 'classic');
      window.__qmChartLifecycle = {idle:new Map(), canceled:new Map(), next:1, disconnects:0};
      window.requestIdleCallback = callback => {
        const id = window.__qmChartLifecycle.next++;
        window.__qmChartLifecycle.idle.set(id, callback);
        return id;
      };
      window.cancelIdleCallback = id => {
        const callback = window.__qmChartLifecycle.idle.get(id);
        if (callback) window.__qmChartLifecycle.canceled.set(id, callback);
        window.__qmChartLifecycle.idle.delete(id);
      };
      window.__runIdle = () => {
        const pending = [...window.__qmChartLifecycle.idle.entries()];
        window.__qmChartLifecycle.idle.clear();
        pending.forEach(([, callback]) => callback({didTimeout:false, timeRemaining:() => 10}));
      };
      window.__runCanceledIdle = () => {
        const pending = [...window.__qmChartLifecycle.canceled.values()];
        window.__qmChartLifecycle.canceled.clear();
        pending.forEach(callback => callback({didTimeout:false, timeRemaining:() => 10}));
      };
      window.IntersectionObserver = class {
        constructor(callback) { this.callback = callback; this.targets = new Set(); }
        observe(target) {
          this.targets.add(target);
          queueMicrotask(() => this.targets.has(target) && this.callback([{target, isIntersecting:true}]));
        }
        unobserve(target) { this.targets.delete(target); }
        disconnect() { this.targets.clear(); window.__qmChartLifecycle.disconnects += 1; }
      };
    """

    with playwright_sync.sync_playwright() as manager:
        browser = manager.chromium.launch()
        page = browser.new_page(viewport={"width": 1280, "height": 900})
        page.add_init_script(lifecycle_script)
        page.route("**/api/v1/market/overview", lambda route: route.fulfill(json=market))
        page.route("**/api/v1/market/fear-greed", lambda route: route.fulfill(json=fear_greed))
        page.goto(url)
        page.wait_for_function("() => window.__qmChartLifecycle.idle.size > 0")
        page.evaluate("window.__runIdle()")
        page.locator('[data-market-group="A股指数"] .spark canvas').wait_for(state="visible")

        page.evaluate("window.QuantMasterShell.loadMarket()")
        page.wait_for_function("() => window.__qmChartLifecycle.idle.size > 0")
        pending = page.evaluate("window.__qmChartLifecycle.idle.size")
        page.get_by_role("button", name="运行", exact=True).click()
        page.wait_for_url(re.compile(r"#runtime/automation$"))
        assert page.evaluate("window.__qmChartLifecycle.disconnects") >= 1
        assert page.evaluate("window.__qmChartLifecycle.canceled.size") >= pending
        assert page.locator("#tab-market canvas, #tab-decision canvas").count() == 0
        page.evaluate("window.__runCanceledIdle()")
        page.wait_for_timeout(50)
        assert page.locator("#tab-market canvas, #tab-decision canvas").count() == 0

        delayed = browser.new_page(viewport={"width": 1280, "height": 900})
        held = []
        delayed.route("**/api/v1/market/overview", lambda route: route.fulfill(json=market))
        delayed.route("**/api/v1/market/fear-greed", lambda route: route.fulfill(json=fear_greed))
        delayed.route("**/static/today-charts.js", lambda route: held.append(route))
        delayed.goto(url)
        delayed.wait_for_url(re.compile(r"#today/quotes$"))
        for _ in range(40):
            if held:
                break
            delayed.wait_for_timeout(25)
        assert len(held) == 1
        delayed.get_by_role("button", name="运行", exact=True).click()
        delayed.wait_for_url(re.compile(r"#runtime/automation$"))
        delayed.evaluate(
            """() => {
              window.__canvasAddsAfterLeave = 0;
              window.__canvasObserver = new MutationObserver(records => records.forEach(record => {
                record.addedNodes.forEach(node => {
                  if (node.nodeType === 1 && (node.matches?.('canvas') || node.querySelector?.('canvas'))) {
                    window.__canvasAddsAfterLeave += 1;
                  }
                });
              }));
              window.__canvasObserver.observe(document.body, {childList:true, subtree:true});
            }"""
        )
        held.pop().continue_()
        delayed.wait_for_timeout(250)
        assert delayed.evaluate("window.__canvasAddsAfterLeave") == 0
        assert delayed.locator("#tab-market canvas, #tab-decision canvas").count() == 0
        browser.close()


def test_workspace_loader_owns_lazy_journeys_and_reuses_modules(live_server):
    url, _ = live_server
    with playwright_sync.sync_playwright() as manager:
        browser = manager.chromium.launch()
        page = browser.new_page(viewport={"width": 1280, "height": 900})
        requested = []
        errors = []
        page.on("request", lambda request: requested.append(request.url.split("?", 1)[0]))
        page.on("pageerror", lambda error: errors.append(str(error)))
        page.goto(url)
        page.wait_for_url(re.compile(r"#today/market\?"))
        page.locator(".market-workbench").wait_for(state="visible")
        initial = set(requested)
        assert f"{url}/static/workspaces/today.js" in initial
        assert not any(name in path for path in initial for name in (
            "/workspaces/research.js", "/workspaces/account.js", "/workspaces/runtime.js",
            "/lab.js", "/rotation.js", "/help.js",
        ))

        journeys = [
            ("研究", "#research/lab", "#tab-lab", "/static/workspaces/research.js"),
            ("账户", "#account/paper", "#tab-paper", "/static/workspaces/account.js"),
            ("运行", "#runtime/automation", "#tab-automation", "/static/workspaces/runtime.js"),
            ("今日", "#today/market", "#tab-market", "/static/workspaces/today.js"),
        ]
        for label, route, selector, resource in journeys:
            page.get_by_role("button", name=label, exact=True).click()
            page.wait_for_url(re.compile(re.escape(route) + (r"\?" if route == "#today/market" else r"$")))
            page.locator(selector).wait_for(state="visible")
            playwright_sync.expect(page.locator(selector)).to_have_class(re.compile(r"(?:^|\s)active(?:\s|$)"))
            assert requested.count(f"{url}{resource}") == 1
            if route == "#research/lab":
                page.wait_for_function("() => typeof window.echarts !== 'undefined'")
                assert requested.count(f"{url}/static/echarts.min.js") == 1

        page.get_by_role("button", name="研究", exact=True).click()
        page.get_by_role("button", name="今日", exact=True).click()
        page.wait_for_url(re.compile(r"#today/market\?"))
        page.locator(".market-workbench").wait_for(state="visible")
        assert requested.count(f"{url}/static/workspaces/research.js") == 1
        assert requested.count(f"{url}/static/workspaces/today.js") == 1

        page.get_by_role("tab", name="行业周期", exact=True).click()
        page.wait_for_url(re.compile(r"#today/industry$"))
        page.locator("#tab-rotation").wait_for(state="visible")
        page.wait_for_function("() => typeof window.echarts !== 'undefined'")
        assert requested.count(f"{url}/static/echarts.min.js") == 1
        assert requested.count(f"{url}/static/rotation.js") == 1

        page.get_by_role("button", name="手册", exact=True).click()
        page.wait_for_url(re.compile(r"#runtime/help$"))
        page.wait_for_timeout(1_000)
        assert errors == []
        page.locator("#help-root .help-handbook").wait_for(state="visible")
        assert requested.count(f"{url}/static/help.js") == 1
        assert requested.count(f"{url}/static/help-content.html") == 1
        assert page.evaluate("window.scrollY") == 0
        assert page.locator(".app-header").bounding_box()["y"] == 0

        page.set_viewport_size({"width": 390, "height": 844})
        assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
        page.get_by_role("button", name="研究", exact=True).click()
        page.wait_for_url(re.compile(r"#research/lab$"))
        page.locator(".lab-head-actions .lab-button").first.wait_for(state="visible")
        for theme in ("classic", "ink"):
            page.evaluate("theme => document.documentElement.dataset.qmTheme = theme", theme)
            action_bounds = page.locator(".lab-head-actions .lab-button").evaluate_all(
                "nodes => nodes.map(node => node.getBoundingClientRect().toJSON())"
            )
            assert len(action_bounds) == 3
            assert all(bound["width"] >= 300 and bound["height"] >= 44 for bound in action_bounds)
        assert errors == []
        browser.close()


def test_owner_view_resource_budgets_use_browser_request_attribution(live_server):
    report = _measure_workspace_resource_budgets(live_server[0], live_server[1])

    assert report["initial_raw_bytes"] <= 1024 * 1024, report
    assert set(report["workspace_unions"]) == {"today", "research", "account", "runtime"}
    for route, view in report["views"].items():
        assert view["owned_bytes"] <= 350 * 1024, {route: view}
    assert 0 < report["echarts_vendor_bytes"] <= 1024 * 1024, report
    assert "/static/echarts.min.js" in report["initial_resources"]
    print("workspace resource budgets: " + json.dumps(report, ensure_ascii=False, sort_keys=True))


def test_workspace_activation_is_latest_wins_after_mount_wait(live_server):
    url, _ = live_server
    with playwright_sync.sync_playwright() as manager:
        browser = manager.chromium.launch()
        page = browser.new_page(viewport={"width": 1280, "height": 900})
        held = []
        page.add_init_script(
            """document.addEventListener('quantmaster:workspace-mounted', event => {
              (window.__workspaceMounts ||= []).push(`${event.detail.workspace}/${event.detail.page}`);
            });"""
        )
        page.route("**/static/lab.js", lambda route: held.append(route))
        page.goto(url)
        page.wait_for_url(re.compile(r"#today/market\?"))
        page.wait_for_function("() => (window.__workspaceMounts || []).includes('today/market')")

        page.get_by_role("button", name="研究", exact=True).click()
        for _ in range(40):
            if held:
                break
            page.wait_for_timeout(25)
        assert len(held) == 1
        page.get_by_role("button", name="运行", exact=True).click()
        held.pop().continue_()

        page.wait_for_url(re.compile(r"#runtime/automation$"))
        page.locator("#tab-automation").wait_for(state="visible")
        page.wait_for_timeout(1_000)
        assert page.evaluate("window.__workspaceMounts") == [
            "today/market", "runtime/automation",
        ]
        playwright_sync.expect(page.get_by_role("button", name="运行", exact=True)).to_have_attribute(
            "aria-current", "page"
        )
        playwright_sync.expect(page.locator("#tab-lab")).to_be_hidden()
        browser.close()


def test_workspace_deep_link_refresh_and_load_failure_are_fail_closed(live_server):
    url, _ = live_server
    with playwright_sync.sync_playwright() as manager:
        browser = manager.chromium.launch()
        page = browser.new_page(viewport={"width": 390, "height": 844})
        errors = []
        page.on("pageerror", lambda error: errors.append(str(error)))
        page.goto(f"{url}/#research/lab")
        page.wait_for_url(re.compile(r"#research/lab$"))
        page.locator("#tab-lab").wait_for(state="visible")
        page.reload()
        page.wait_for_url(re.compile(r"#research/lab$"))
        page.locator("#tab-lab").wait_for(state="visible")
        assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
        assert errors == []

        failed = browser.new_page()
        account_requests = 0
        market_requests = 0

        def count_market(route):
            nonlocal market_requests
            market_requests += 1
            route.continue_()

        def fail_account_once(route):
            nonlocal account_requests
            account_requests += 1
            if account_requests == 1:
                route.abort()
            else:
                route.continue_()

        failed.route("**/static/workspaces/account.js*", fail_account_once)
        failed.route("**/api/v1/market/overview", count_market)
        failed.goto(url)
        failed.locator(".market-workbench").wait_for(state="visible")
        initial_market_requests = market_requests
        failed.get_by_role("button", name="账户", exact=True).click()
        alert = failed.get_by_role("alert")
        playwright_sync.expect(alert).to_contain_text("工作区加载失败")
        playwright_sync.expect(failed.locator("#tab-market")).to_have_class(
            re.compile(r"(?:^|\s)active(?:\s|$)")
        )
        assert "#today/market?" in failed.url
        assert market_requests == initial_market_requests
        failed.get_by_role("button", name="账户", exact=True).click()
        failed.wait_for_url(re.compile(r"#account/paper$"))
        failed.locator("#tab-paper").wait_for(state="visible")
        assert account_requests == 2
        browser.close()


def test_workspace_mount_failure_restores_previous_route_and_retries_style(live_server):
    url, _ = live_server
    with playwright_sync.sync_playwright() as manager:
        browser = manager.chromium.launch()
        page = browser.new_page(viewport={"width": 1280, "height": 900})
        style_requests = 0

        def fail_style_once(route):
            nonlocal style_requests
            style_requests += 1
            if style_requests == 1:
                route.abort()
            else:
                route.continue_()

        page.route("**/static/lab.css*", fail_style_once)
        page.goto(url)
        page.wait_for_url(re.compile(r"#today/market\?"))
        assert page.evaluate("Object.isFrozen(window.QuantMasterShell)")
        page.locator(".market-workbench").wait_for(state="visible")

        page.get_by_role("button", name="研究", exact=True).click()
        playwright_sync.expect(page.get_by_role("alert")).to_contain_text("工作区加载失败")
        assert "#today/market?" in page.url
        playwright_sync.expect(page.get_by_role("button", name="今日", exact=True)).to_have_class(
            re.compile(r"(?:^|\s)active(?:\s|$)")
        )
        playwright_sync.expect(page.locator("#tab-market")).to_be_visible()

        page.get_by_role("button", name="研究", exact=True).click()
        page.wait_for_url(re.compile(r"#research/lab$"))
        page.locator("#tab-lab").wait_for(state="visible")
        assert style_requests == 2
        assert page.locator('link[href="/static/lab.css"]').count() == 1
        playwright_sync.expect(page.get_by_role("alert")).to_be_hidden()
        browser.close()


def test_workspace_mount_resource_failures_retry_script_and_today_feature(live_server):
    url, _ = live_server
    with playwright_sync.sync_playwright() as manager:
        browser = manager.chromium.launch()

        script_page = browser.new_page(viewport={"width": 1280, "height": 900})
        script_requests = 0

        def fail_script_once(route):
            nonlocal script_requests
            script_requests += 1
            if script_requests == 1:
                route.abort()
            else:
                route.continue_()

        script_page.route("**/static/echarts.min.js*", fail_script_once)
        script_page.goto(f"{url}/#runtime/automation")
        script_page.wait_for_url(re.compile(r"#runtime/automation$"))
        script_page.locator("#tab-automation").wait_for(state="visible")
        script_page.get_by_role("button", name="研究", exact=True).click()
        playwright_sync.expect(script_page.get_by_role("alert")).to_contain_text("工作区加载失败")
        assert script_page.url.endswith("#runtime/automation")
        script_page.get_by_role("button", name="研究", exact=True).click()
        script_page.wait_for_url(re.compile(r"#research/lab$"))
        script_page.locator("#tab-lab").wait_for(state="visible")
        assert script_requests == 2
        assert script_page.locator('script[src="/static/echarts.min.js"]').count() == 1

        feature_page = browser.new_page(viewport={"width": 1280, "height": 900})
        feature_requests = 0

        def fail_feature_once(route):
            nonlocal feature_requests
            feature_requests += 1
            if feature_requests == 1:
                route.abort()
            else:
                route.continue_()

        feature_page.route("**/static/rotation.js*", fail_feature_once)
        feature_page.goto(url)
        feature_page.wait_for_url(re.compile(r"#today/market\?"))
        feature_page.get_by_role("tab", name="行业周期", exact=True).click()
        playwright_sync.expect(feature_page.get_by_role("alert")).to_contain_text("工作区加载失败")
        assert "#today/market?" in feature_page.url
        playwright_sync.expect(feature_page.locator("#tab-market")).to_be_visible()
        feature_page.get_by_role("tab", name="行业周期", exact=True).click()
        feature_page.wait_for_url(re.compile(r"#today/industry$"))
        feature_page.locator("#tab-rotation").wait_for(state="visible")
        assert feature_requests == 2

        browser.close()


def test_live_server_teardown_prefers_graceful_lifespan_signal() -> None:
    events = []

    class Process:
        @staticmethod
        def poll():
            return None

        @staticmethod
        def send_signal(value):
            events.append(("signal", value))

        @staticmethod
        def wait(timeout):
            events.append(("wait", timeout))
            return 0

        @staticmethod
        def terminate():
            events.append(("terminate", None))

        @staticmethod
        def kill():
            events.append(("kill", None))

    _stop_live_server(Process(), timeout=7)

    expected = signal.CTRL_BREAK_EVENT if os.name == "nt" else signal.SIGINT
    assert events == [("signal", expected), ("wait", 7)]


def test_live_server_fixture_disables_worker_supervisor() -> None:
    source = Path(__file__).read_text(encoding="utf-8")
    assert 'env["QM_DISABLE_WORKER_SUPERVISOR"] = "1"' in source


def test_factor_robustness_deep_link_renders_frozen_charts_and_tables(live_server):
    url, _ = live_server
    evidence = {
        "schema_version": 2,
        "status": "pass",
        "factor": {
            "version_id": "test-version", "version": 2, "name": "动量稳健性",
            "slug": "momentum", "description": "检验动量在不同路径和市场状态下的稳定性",
        },
        "horizon": 3,
        "available_horizons": [3, 5],
        "validation": {
            "created_at": "2026-08-15T02:00:00Z", "dataset_hash": "frozen-snapshot",
            "protocol": {"train_window": 756, "test_window": 244},
        },
        "metrics": {
            "oos_days": 244, "oos_rank_ic": 0.032, "oos_icir": 0.45,
            "retention": 0.72, "positive_ratio": 0.58, "candidate_score": 74,
        },
        "summary": {"tests_passed": 4, "tests_applicable": 4, "failed_tests": []},
        "sections": [
            {
                "key": "monte_carlo", "status": "pass", "title": "Monte Carlo 区块自助法",
                "explanation": "重复抽样检验路径稳定性。", "action": "保留当前证据。",
                "evidence": {
                    "available": True, "passed": True, "paths": 500, "block_days": 20,
                    "probability_positive_ic": 0.96, "probability_positive_net": 0.82,
                    "ic_mean_ci_95": [0.012, 0.051],
                    "thresholds": {"probability_positive_ic": 0.9},
                    "ic_mean_distribution": {
                        "quantiles": [
                            {"probability": 0.05, "value": 0.018},
                            {"probability": 0.5, "value": 0.032},
                        ],
                        "histogram": [
                            {"start": 0.01, "end": 0.03, "count": 180, "share": 0.36},
                            {"start": 0.03, "end": 0.05, "count": 320, "share": 0.64},
                        ],
                    },
                    "net_annual_distribution": {
                        "quantiles": [{"probability": 0.5, "value": 0.14}],
                        "histogram": [
                            {"start": -0.02, "end": 0.10, "count": 90, "share": 0.18},
                            {"start": 0.10, "end": 0.22, "count": 410, "share": 0.82},
                        ],
                    },
                },
            },
            {
                "key": "parameter_sensitivity", "status": "pass", "title": "参数敏感性",
                "explanation": "检验参数平台。", "action": "保留邻域证据。",
                "evidence": {
                    "available": True, "applicable": True, "passed": True,
                    "thresholds": {"same_sign_ratio": 0.75},
                    "variants": [{
                        "variant": "window:20→16", "rank_ic": 0.029, "retention": 0.91,
                        "same_sign": True, "factor_rank_correlation": 0.88,
                    }],
                },
            },
            {
                "key": "walk_forward", "status": "pass", "title": "Walk-forward 分析",
                "explanation": "过去训练未来测试。", "action": "不得回看密封窗口。",
                "evidence": {
                    "available": True, "passed": True,
                    "thresholds": {"sign_consistency": 0.75},
                    "folds": [{
                        "train_start": "2018-01-01", "train_end": "2020-12-31",
                        "test_start": "2021-01-01", "test_end": "2021-12-31",
                        "train_rank_ic": 0.04, "rank_ic": 0.031, "retention": 0.78,
                    }],
                    "sealed": {
                        "train_start": "2019-01-01", "train_end": "2022-12-31",
                        "test_start": "2023-01-01", "test_end": "2023-12-31",
                        "train_rank_ic": 0.038, "rank_ic": 0.029, "retention": 0.76,
                    },
                },
            },
            {
                "key": "penetration", "status": "pass", "title": "穿透性测试",
                "explanation": "拆解条件依赖。", "action": "持续观察弱分层。",
                "evidence": {
                    "available": True, "passed": True,
                    "thresholds": {"effective_names": 8},
                    "time": {"years": [{"year": 2024, "rank_ic": 0.031}]},
                    "regimes": {"buckets": [{"regime": "downtrend", "rank_ic": 0.021}]},
                    "liquidity": {"buckets": [{"bucket": "low", "rank_ic": 0.026}]},
                    "concentration": {
                        "top1_absolute_contribution_share": 0.12,
                        "top5_absolute_contribution_share": 0.42,
                        "effective_names": 14, "symbols": 320,
                        "top_contributors": [{"symbol": "000001.SZ", "share": 0.12}],
                    },
                },
            },
        ],
    }
    with playwright_sync.sync_playwright() as manager:
        browser = manager.chromium.launch()
        page = browser.new_page(viewport={"width": 1280, "height": 900})
        page.route(
            "**/api/v1/lab/factors/test-version/robustness?*",
            lambda route: route.fulfill(status=200, json=evidence),
        )
        page.goto(
            f"{url}/?lab_version=test-version&lab_horizon=3#research/lab",
            wait_until="domcontentloaded",
        )

        detail = page.locator('[data-lab-panel="robustness"]')
        playwright_sync.expect(detail).to_have_class(re.compile(r"(?:^|\s)active(?:\s|$)"))
        _wait_for_text(page.locator("#lab-robustness-title"), "动量稳健性 · 3 日鲁棒性")
        playwright_sync.expect(page.locator(".lab-robust-section")).to_have_count(4)
        playwright_sync.expect(page.locator(".lab-histogram i")).to_have_count(4)
        _wait_for_text(detail, "2023-01-01 → 2023-12-31")
        _wait_for_text(detail, "000001.SZ")
        assert "lab_version=test-version" in page.url
        page.locator("[data-robustness-back]").click()
        playwright_sync.expect(page.locator('[data-lab-panel="library"]')).to_have_class(
            re.compile(r"(?:^|\s)active(?:\s|$)"),
        )
        assert "lab_version" not in page.url
        browser.close()


def test_settings_candidate_and_csv_flow(live_server, tmp_path):
    url, _ = live_server
    with playwright_sync.sync_playwright() as manager:
        browser = manager.chromium.launch()
        page = browser.new_page(viewport={"width": 1360, "height": 900})
        page.goto(url)
        release = page.locator("#release-trigger")
        release.wait_for(state="visible")
        assert f"v{__version__}" in release.inner_text()
        assert RELEASE_DATE.replace("-", ".") in release.inner_text()
        release.click()
        page.locator("#release-popover").wait_for(state="visible")
        assert "更新日志" in page.locator("#release-popover").inner_text()
        assert page.get_by_role("button", name="立即热更新").count() == 0
        assert page.locator("#release-reload-status").count() == 0
        assert page.locator("#release-popover #free-stockdb-release").count() == 0
        stockdb = page.locator("#stockdb-update-trigger")
        stockdb.click()
        page.locator("#stockdb-update-popover").wait_for(state="visible")
        assert page.locator("#release-popover").is_hidden()
        assert "stockdb 数据状态" in page.locator("#stockdb-update-popover").inner_text()
        assert page.locator("#stockdb-update-popover #free-stockdb-release").count() == 1

        settings = page.get_by_role("button", name="设置", exact=True)
        assert settings.bounding_box()["x"] > page.locator("#nav").bounding_box()["x"]
        assert settings.inner_text() == ""
        assert settings.locator(".settings-gear").count() == 1
        assert page.locator("#nav.workspace-nav button").all_inner_texts() == [
            "今日",
            "研究",
            "账户",
            "运行",
        ]
        assert page.locator('[data-workspace-pages="today"] button').all_inner_texts() == [
            "市场全景",
            "行情",
            "市场温度",
            "市场风格",
            "轮动总览",
            "行业周期",
            "细分题材",
            "ETF 研究",
            "资讯",
            "盘后扫描",
            "候选",
            "个股分析",
            "决策",
        ]
        page.get_by_role("button", name="研究", exact=True).click()
        page.wait_for_url(re.compile(r"#research/lab$"))
        assert page.url.endswith("#research/lab")
        page.locator("#tab-lab").wait_for(state="visible")
        page.get_by_role("button", name="账户", exact=True).click()
        page.wait_for_url(re.compile(r"#account/paper$"))
        assert page.url.endswith("#account/paper")
        page.locator("#tab-paper").wait_for(state="visible")
        page.get_by_role("button", name="运行", exact=True).click()
        page.wait_for_url(re.compile(r"#runtime/automation$"))
        assert page.url.endswith("#runtime/automation")
        runtime_pages = page.locator('[data-workspace-pages="runtime"]')
        assert runtime_pages.is_visible()
        assert runtime_pages.locator("button").all_inner_texts() == ["任务与消息", "运维更新"]
        settings.click()
        config_path = page.locator("#settings-config-path")
        config_path.wait_for(state="visible")
        playwright_sync.expect(config_path).not_to_have_text("正在读取配置…")
        assert page.locator("#settings-nav .settings-nav-group").count() == 5
        assert page.locator("#settings-nav [data-settings-section]").count() == 11
        assert page.locator("#settings-nav [data-settings-section]").all_inner_texts() == [
            "模型服务",
            "在线数据源",
            "本地行情库",
            "研究数据",
            "Quant Lab",
            "交易规则",
            "自动化",
            "资讯处理",
            "资讯来源",
            "本机服务",
            "快照回滚",
        ]

        page.locator('#settings-nav [data-settings-section="online-data"]').click()
        page.locator('[data-settings-panel="online-data"]').wait_for(state="visible")
        diagnostic_cards = page.locator(
            '[data-settings-panel="online-data"] .settings-diagnostic-grid .settings-diagnostic'
        )
        diagnostic_cards.nth(0).locator(".check-result").evaluate(
            "element => { element.style.minHeight = '240px'; }"
        )
        card_layout = diagnostic_cards.evaluate_all(
            "elements => elements.map(element => "
            "({top: element.offsetTop, height: element.offsetHeight}))"
        )
        assert card_layout[0]["top"] == card_layout[1]["top"]
        assert card_layout[0]["height"] > card_layout[1]["height"]

        page.locator('#settings-nav [data-settings-section="automation"]').click()
        page.locator('[data-settings-panel="automation"]').wait_for(state="visible")
        desktop_lists = page.locator(".automation-list-field").evaluate_all(
            "elements => elements.map(element => "
            "({top: element.offsetTop, left: element.offsetLeft}))"
        )
        assert desktop_lists[0]["top"] == desktop_lists[1]["top"]
        assert desktop_lists[0]["left"] < desktop_lists[1]["left"]
        page.locator('#settings-nav [data-settings-section="llm"]').click()

        browser_settings = page.evaluate(
            "async () => structuredClone((await import('/static/settings.js')).state.config)"
        )

        def fulfill_settings_save(route):
            body = route.request.post_data_json
            if route.request.method != "PUT":
                route.continue_()
                return
            for key in (
                "config_version",
                "llm",
                "data",
                "trade",
                "news",
                "server",
                "automation",
                "lab",
            ):
                if key in body:
                    browser_settings[key] = body[key]
            browser_settings["managed_by_gui"] = True
            route.fulfill(
                status=200,
                content_type="application/json",
                body=json.dumps(
                    {
                        "status": "ok",
                        "warnings": [],
                        "changed_fields": [],
                        "restart_required": [],
                        "apply_status": {},
                        "runtime": browser_settings["runtime"],
                        "settings": browser_settings,
                    }
                ),
            )

        page.route(
            "**/api/v1/settings/validate",
            lambda route: route.fulfill(
                status=200,
                content_type="application/json",
                body=json.dumps({
                    "normalized": route.request.post_data_json,
                    "warnings": [],
                }),
            ),
        )
        page.route("**/api/v1/settings", fulfill_settings_save)

        page.locator('[name="llm.provider"]').select_option("openai-compatible")
        page.locator('[name="llm.base_url"]').fill("http://127.0.0.1:9/v1")
        page.locator('[name="llm.model"]').fill("manual-local-model")
        page.locator('[name="llm.reasoning_effort"]').select_option("high")
        page.locator('[name="llm.max_concurrency"]').fill("2")
        page.locator('[name="llm.max_concurrency"]').blur()
        invalid_fields = page.locator("#settings-form :invalid").evaluate_all(
            "elements => elements.map(element => element.name || element.id)"
        )
        assert invalid_fields == []
        _wait_for_class(page.locator("#settings-save-state"), "saved")
        page.locator('[data-check="llm-models"]').click()
        model_check = page.locator('[data-check-result="llm-models"]')
        playwright_sync.expect(model_check).to_have_class(
            re.compile(r"(?:^|\s)(?:error|warning)(?:\s|$)"),
            timeout=30_000,
        )
        check_text = model_check.inner_text()
        assert check_text.strip()
        assert "检测中" not in check_text
        assert page.locator('[name="llm.model"]').input_value() == "manual-local-model"
        assert page.locator('[name="llm.reasoning_effort"]').input_value() == "high"
        assert page.locator('[name="llm.max_concurrency"]').input_value() == "2"
        assert (
            model_check.locator("xpath=ancestor::section[1]").get_attribute("data-diagnostic") == "llm-models"
        )
        page.locator('[name="llm.timeout"]').fill("179")
        playwright_sync.expect(model_check).to_have_class(re.compile(r"(?:^|\s)stale(?:\s|$)"))
        playwright_sync.expect(model_check.locator(".check-stale")).to_be_visible()
        page.locator('[name="llm.timeout"]').blur()
        _wait_for_class(page.locator("#settings-save-state"), "saved")

        page.route(
            "**/api/v1/settings/check/data-sources",
            lambda route: route.fulfill(
                status=200,
                content_type="application/json",
                body=json.dumps(
                    {
                        "status": "warning",
                        "message": "数据源检测完成",
                        "latency_ms": 24,
                        "checked_at": "2026-08-08T01:14:56Z",
                        "details": {
                            "sources": {
                                "akshare": {"status": "success", "message": "端点可达"},
                                "yfinance": {"status": "warning", "message": "端点限流"},
                            },
                            "circuits": {"yahoo": {"state": "open"}},
                            "security_master": {
                                "status": "success",
                                "record_count": 33984,
                                "coverage": [{"market": "CN", "asset_type": "stock", "count": 5871}],
                            },
                            "proxies": {},
                        },
                    }
                ),
            ),
        )
        page.locator('[data-settings-section="online-data"]').click()
        data_diagnostic = page.locator('[data-diagnostic="data-sources"]')
        assert data_diagnostic.locator('[data-check="data-sources"]').count() == 1
        assert data_diagnostic.locator('[data-check-result="data-sources"]').count() == 1
        data_diagnostic.locator('[data-check="data-sources"]').click()
        source_result = data_diagnostic.locator('[data-check-result="data-sources"]')
        playwright_sync.expect(source_result.locator(".check-details")).to_have_attribute("open", "")
        assert source_result.locator(".check-detail-groups h5").all_inner_texts() == [
            "依赖与端点",
            "熔断状态",
            "证券主数据",
            "代理",
        ]

        page.locator('[data-settings-section="automation"]').click()
        assert page.locator('[name="automation.enabled"]').is_visible()
        page.locator('[name="automation.retention_days"]').fill("120")
        _wait_for_class(page.locator("#settings-save-state"), "saved")

        page.locator('[data-settings-section="backup"]').click()
        page.locator('#snapshot-form [name="name"]').fill("UI baseline")
        with page.expect_response(
            lambda response: (
                response.request.method == "POST"
                and response.url == f"{url}/api/v1/settings/snapshots"
            ),
        ) as snapshot_response:
            page.locator("#snapshot-form button").click()
        assert snapshot_response.value.status == 200
        page.get_by_text("UI baseline", exact=True).first.wait_for()

        page.locator('header [data-tab="candidates"]').evaluate("element => element.click()")
        page.locator(".candidate-detail").wait_for()
        page.locator("#candidate-new").click()
        preset_buttons = page.locator("[data-candidate-index-preset]")
        assert preset_buttons.count() == 9
        assert preset_buttons.nth(0).get_attribute("data-candidate-index-preset") == "000688.SH"
        assert "科创50" in preset_buttons.nth(0).inner_text()
        assert "中证1000" in preset_buttons.nth(8).inner_text()
        page.locator("#candidate-new-name").fill("ui_candidate")
        page.get_by_role("button", name="添加代码", exact=True).click()
        instrument_input = page.locator("#candidate-add-symbol")
        instrument_input.fill("700")
        page.locator("#candidate-instrument-options").wait_for(state="visible")
        choices = page.locator("#candidate-instrument-options").inner_text()
        assert "000700.SZ" in choices and "00700.HK" in choices
        instrument_input.press("ArrowDown")
        instrument_input.press("Enter")
        assert instrument_input.input_value() == "00700.HK"
        page.get_by_role("button", name="收起", exact=True).click()
        page.get_by_role("button", name="批量编辑", exact=True).click()
        page.locator("#candidate-bulk-text").fill("600519\n000001\n600519.SH")
        page.get_by_role("button", name="应用到草稿", exact=True).click()
        # 000001 is both the SSE composite index and a Shenzhen stock.  The
        # identity contract deliberately requires an explicit user choice.
        resolution = page.locator(
            '.candidate-resolution [data-candidate-query="000001"]'
            '[data-candidate-choice="000001.SZ"]'
        )
        resolution.wait_for(state="visible")
        resolution.click()
        page.get_by_text("有尚未生效的更改", exact=True).wait_for()
        member_symbols = page.locator(".candidate-member-symbol")
        playwright_sync.expect(member_symbols).to_have_count(2)
        assert member_symbols.all_inner_texts() == [
            "600519.SH",
            "000001.SZ",
        ]
        page.locator('header [data-workspace="account"]').click()
        page.get_by_role("heading", name="先处理尚未生效的更改", exact=True).wait_for()
        assert page.url.endswith("#today/candidates")
        page.get_by_role("button", name="继续编辑", exact=True).click()
        page.get_by_role("button", name="创建候选", exact=True).click()
        page.get_by_role("heading", name="ui_candidate", exact=True).wait_for()
        page.locator('[data-candidate-name="ui_candidate"]').wait_for(state="visible")

        csv = tmp_path / "broker.csv"
        csv.write_text(
            "成交日期,证券代码,买卖方向,成交价格,成交数量,佣金\n"
            "2024-01-02,600519,买入,10,100,5\n"
            "坏日期,000001,卖出,11,100,5\n",
            encoding="utf-8-sig",
        )
        page.locator('header [data-workspace="account"]').click()
        account_pages = page.locator('header [data-workspace-pages="account"]')
        playwright_sync.expect(account_pages).to_be_visible()
        account_pages.locator('[data-tab="ledger"]').click()
        ledger_tab = page.locator("#tab-ledger")
        playwright_sync.expect(ledger_tab).to_be_visible()
        playwright_sync.expect(ledger_tab).to_have_class(
            re.compile(r"(?:^|\s)active(?:\s|$)"),
        )
        page.locator("#broker-csv").set_input_files(csv)
        preview_button = page.locator("#csv-preview-form button")
        preview_button.wait_for(state="visible")
        preview_button.click()
        page.locator("#csv-submit-actions").wait_for(state="visible")
        _wait_for_text(page.locator("#csv-preview"), "坏行")
        page.locator("#csv-submit").click()
        _wait_for_text(page.locator("#csv-import-status"), "未导入")
        page.locator('[name="csv-mode"][value="valid"]').check()
        page.locator("#csv-submit").click()
        _wait_for_text(page.locator("#csv-import-status"), "已导入 1 笔")
        assert page.locator("#csv-download-errors").is_visible()

        page.set_viewport_size({"width": 390, "height": 844})
        page.locator('header [data-tab="candidates"]').evaluate("element => element.click()")
        assert page.locator("#candidate-mobile-select").is_visible()
        assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
        page.get_by_role("button", name="设置", exact=True).click()
        mobile_settings = page.locator("#settings-section-select")
        mobile_settings.wait_for(state="visible")
        assert page.locator("#settings-nav").is_hidden()
        mobile_settings.select_option("automation")
        mobile_lists = page.locator(".automation-list-field").evaluate_all(
            "elements => elements.map(element => "
            "({top: element.offsetTop, left: element.offsetLeft}))"
        )
        assert mobile_lists[0]["left"] == mobile_lists[1]["left"]
        assert mobile_lists[0]["top"] < mobile_lists[1]["top"]
        mobile_settings.select_option("local-data")
        assert page.locator('[data-settings-panel="local-data"]').is_visible()
        columns = page.locator(".settings-shell").evaluate("el => getComputedStyle(el).gridTemplateColumns")
        assert columns != "196px"
        assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
        browser.close()


def test_runtime_lifecycle_snapshot_is_compact_sanitized_and_backward_compatible(live_server):
    url, _ = live_server
    lifecycle = {
        "state": "draining",
        "generation": "17",
        "phase": "finish_atomic_unit",
        "task_counts": {"active": 3, "converging": 2, "handoff": 1},
        "durable_queue": {"pending": 8},
        "deadline": {"phase": "persist_checkpoint", "remaining_seconds": 12},
        "timeout_issues": [
            {
                "diagnostic_id": "QM-SHUTDOWN-014",
                "component": "provider-executor",
                "phase": "stop_executor",
                "detail": "Bearer should-not-render token=also-secret; 在途调用超过阶段时限",
            }
        ],
    }
    diagnostics = {
        "issues": [],
        "runtime": {
            "lifecycle": lifecycle,
            "readiness": {"web_bound": True, "storage_ready": True},
            "web": {"host": "127.0.0.1", "port": 8686, "pid": 12, "generation": "17"},
            "supervisor": {"status": "running", "available": True},
            "storage": {"status": "ready", "data_root": "local"},
            "scheduler": {"status": "stopping", "managed_by": "runtime-worker"},
        },
    }

    with playwright_sync.sync_playwright() as manager:
        browser = manager.chromium.launch()
        page = browser.new_page(viewport={"width": 390, "height": 844})
        page.route(
            "**/api/v1/diagnostics",
            lambda route: route.fulfill(status=200, json=diagnostics),
        )
        page.goto(url)
        panel = page.locator("#runtime-lifecycle")
        panel.wait_for(state="attached")
        page.wait_for_function("!document.querySelector('#runtime-lifecycle').hidden")
        page.locator("#runtime-summary").click()

        assert panel.get_attribute("data-state") == "draining"
        assert "正在收敛" in panel.inner_text()
        assert "generation 17" in panel.inner_text()
        assert "finish_atomic_unit" in panel.inner_text()
        assert "剩余 12 秒" in panel.inner_text()
        assert page.locator("#runtime-lifecycle-active").inner_text() == "3"
        assert page.locator("#runtime-lifecycle-converging").inner_text() == "2"
        assert page.locator("#runtime-lifecycle-handoff").inner_text() == "1"
        assert page.locator("#runtime-lifecycle-queue").inner_text() == "8"
        page.locator("#runtime-lifecycle-issues summary").click()
        issue_text = page.locator("#runtime-lifecycle-issue-list").inner_text()
        assert "QM-SHUTDOWN-014" in issue_text
        assert "should-not-render" not in issue_text
        assert "also-secret" not in issue_text
        assert "***" in issue_text
        assert panel.get_attribute("data-level") == "warning"

        entry_count = page.locator("#runtime-list .runtime-entry").count()
        page.evaluate(
            "runtime => window.QuantMasterRunInfo.syncRuntime(runtime)",
            diagnostics["runtime"],
        )
        assert page.locator("#runtime-list .runtime-entry").count() == entry_count
        assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")

        page.evaluate("window.QuantMasterRunInfo.syncRuntime({})")
        assert page.locator("#runtime-lifecycle").is_hidden()
        for state, label in (
            ("running", "运行中"),
            ("draining", "正在收敛"),
            ("stopping", "正在停止"),
            ("reloading", "正在重载"),
        ):
            page.evaluate(
                "value => window.QuantMasterRunInfo.renderLifecycle(value)",
                {"status": state, "generation_id": "18", "current_phase": "handoff"},
            )
            assert page.locator("#runtime-lifecycle-state").inner_text() == label
        browser.close()


def test_market_temporal_status_is_explicit_accessible_and_narrow(live_server):
    url, _ = live_server
    diagnostics = {
        "issues": [], "recent_recovered": [],
        "runtime": {"readiness": {}, "web": {}, "supervisor": {}, "storage": {}, "scheduler": {}},
        "components": {"market_sessions": {
            "CN": {
                "market_timezone": "Asia/Shanghai", "session_date": "2026-08-13",
                "session_phase": "post_close", "latest_complete_session": "2026-08-12",
                "next_session": "", "next_session_reason": "未提供经验证的未来交易日历",
                "completion_state": "current_session_provider_published_waiting_ingest",
                "provider_state": "published", "provider_published_at": "2026-08-13T15:08:00+08:00",
                "ingest_state": "waiting", "ingested_at": "",
                "ingest_latency_seconds": None, "late_record_count": 49,
                "diagnostic_codes": ["SESSION_WAITING_INGEST", "DATA_LATE"],
            },
            "HK": {
                "market_timezone": "Asia/Hong_Kong", "completion_state": "calendar_unavailable",
                "next_session_reason": "未提供经验证的未来交易日历",
                "provider_state": "unavailable", "ingest_state": "unavailable",
                "diagnostic_codes": ["CALENDAR_UNVERIFIED", "TIME_UNZONED"],
            },
            "US": {
                "market_timezone": "America/New_York", "completion_state": "calendar_unavailable",
                "next_session_reason": "未提供经验证的未来交易日历",
                "provider_state": "unavailable", "ingest_state": "unavailable",
                "diagnostic_codes": ["TIME_UNINTERPRETABLE"],
            },
        }},
    }

    with playwright_sync.sync_playwright() as manager:
        browser = manager.chromium.launch()
        page = browser.new_page(viewport={"width": 320, "height": 700})
        page.route("**/api/v1/diagnostics", lambda route: route.fulfill(status=200, json=diagnostics))
        page.goto(url)
        page.locator("#runtime-summary").click()
        markets = page.locator("#runtime-markets")
        playwright_sync.expect(markets).to_have_attribute("aria-live", "polite")
        playwright_sync.expect(page.locator("#runtime-market-list")).to_have_attribute("aria-busy", "false")
        assert page.locator(".runtime-market").count() == 3
        cn = page.locator('[data-market="CN"]')
        assert "等待本地完整摄取" in cn.locator("summary").inner_text()
        cn.locator("summary").focus()
        page.keyboard.press("Enter")
        cn_text = cn.inner_text()
        assert "Asia/Shanghai" in cn_text
        assert "最近完整日线\n2026-08-12" in cn_text
        assert "下一 session\n不可用 · 未提供经验证的未来交易日历" in cn_text
        assert "等待摄取" in cn_text
        assert "49 条" in cn_text
        assert "SESSION_WAITING_INGEST" in cn_text
        hk = page.locator('[data-market="HK"]')
        hk.locator("summary").click()
        assert "TIME_UNZONED" in hk.inner_text()
        us = page.locator('[data-market="US"]')
        us.locator("summary").click()
        assert "America/New_York" in us.inner_text()
        assert "TIME_UNINTERPRETABLE" in us.inner_text()
        assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
        browser.close()


def test_cache_namespace_observability_is_progressive_accessible_and_narrow(live_server):
    url, _ = live_server
    cache = {
        "summary": {
            "namespace_count": 1, "observed_count": 1, "hit_rate": 0.75,
            "fresh": 7, "stale": 1, "partial": 2, "negative": 1,
            "pending": 2, "provider_revalidation_pending": 1,
        },
        "namespaces": [{
            "namespace": "market.bars", "label": "行情", "observed": True,
            "value_kind": "normalized OHLCV bars",
            "freshness_rule": "交易日与收盘边界，formal read 受 as_of 约束",
            "dependencies": ["provider_config", "parser", "calendar"],
            "hit_rate": 0.75, "hits": 3, "misses": 1,
            "counts": {"fresh": 7, "stale": 1, "partial": 2, "negative": 1},
            "oldest_at": "2026-08-01T00:00:00Z", "newest_at": "2026-08-13T07:00:00Z",
            "refresh": {"completed": 8, "total": 10, "pending": 2},
            "negatives": [{"reason": "instrument_not_found", "count": 1}],
            "stale_consumers": ["市场页"], "provider_revalidation_pending": 1,
            "config_revision": "cfg-12", "parser_revision": "bars-v4",
            "issues": [{"code": "CACHE_PARTIAL", "message": "2 项待补齐"}],
        }],
    }
    diagnostics = {
        "issues": [], "recent_recovered": [], "cache": cache,
        "runtime": {"readiness": {}, "web": {}, "supervisor": {}, "storage": {}, "scheduler": {}},
    }

    with playwright_sync.sync_playwright() as manager:
        browser = manager.chromium.launch()
        page = browser.new_page(viewport={"width": 390, "height": 844})
        page.route(
            "**/api/v1/diagnostics",
            lambda route: route.fulfill(status=200, json=diagnostics),
        )
        page.goto(url)
        page.get_by_role("button", name="设置", exact=True).click()
        page.locator("#settings-section-select").select_option("local-data")
        state = page.locator("#cache-observability-state")
        _wait_for_text(state, "已观测 1 / 1")
        assert "75.0%" in page.locator("#cache-observability-summary").inner_text()
        namespace = page.locator(".cache-namespace")
        assert "需关注" in namespace.locator("summary").inner_text()
        assert namespace.get_attribute("data-cache-state") == "attention"
        namespace.locator("summary").focus()
        page.keyboard.press("Enter")
        assert "formal read 受 as_of 约束" in namespace.inner_text()
        assert "instrument_not_found · 1 项" in namespace.inner_text()
        assert "CACHE_PARTIAL" in namespace.inner_text()
        assert "市场页" in namespace.inner_text()
        assert "cfg-12 / bars-v4" in namespace.inner_text()
        _wait_for_document_fit(page)
        browser.close()


def test_help_handbook_search_routes_and_calculators(live_server):
    url, _ = live_server
    trade_settings = {
        "commission_rate": 0.00025,
        "commission_min": 5.0,
        "stamp_tax_rate": 0.0005,
        "transfer_fee_rate": 0.00001,
        "slippage": 0.001,
        "lot_size": 100,
    }
    with playwright_sync.sync_playwright() as manager:
        browser = manager.chromium.launch()
        page = browser.new_page(viewport={"width": 1360, "height": 900})
        page.add_init_script(
            """(() => {
              const NativeObserver = window.IntersectionObserver;
              window.__helpObservers = {created: 0, disconnected: 0};
              window.IntersectionObserver = function(...args) {
                const observer = new NativeObserver(...args);
                window.__helpObservers.created += 1;
                const disconnect = observer.disconnect.bind(observer);
                observer.disconnect = () => {
                  window.__helpObservers.disconnected += 1;
                  disconnect();
                };
                return observer;
              };
              window.IntersectionObserver.prototype = NativeObserver.prototype;
            })();"""
        )
        page.route(
            "**/api/v1/settings",
            lambda route: route.fulfill(
                status=200,
                content_type="application/json",
                body=json.dumps({"trade": trade_settings}),
            ),
        )
        page.goto(url)

        help_button = page.get_by_role("button", name="手册", exact=True)
        settings_button = page.get_by_role("button", name="设置", exact=True)
        assert help_button.bounding_box()["x"] < settings_button.bounding_box()["x"]
        help_button.click()
        page.locator("#help-start").wait_for(state="visible")
        assert page.evaluate("window.__helpObservers.created") == 1
        _wait_for_text(page.locator("#help-settings-status"), "已载入")
        assert page.locator("#help-settings-status").inner_text().startswith("已载入")
        assert page.locator("#help-article h2").count() == 28
        assert page.locator(".help-sidebar .help-nav-part").count() == 6
        assert page.locator(".help-sidebar .help-nav-part > ol").count() == 6
        assert page.evaluate("location.hash") == "#runtime/help"

        page.reload()
        page.locator("#help-start").wait_for(state="visible")
        assert page.locator("#tab-help").evaluate("el => el.classList.contains('active')")

        page.evaluate(
            """() => new Promise(resolve => {
              document.addEventListener('scrollend', resolve, {once:true});
              document.querySelector('[data-help-link="validation"]').click();
            })"""
        )
        page.locator("#help-validation").wait_for(state="visible")
        playwright_sync.expect(page.locator('[data-help-link="validation"]')).to_have_attribute(
            "aria-current",
            "location",
        )

        search = page.locator("#help-search-input")
        search.fill("T+1")
        page.locator(".help-search-result").first.wait_for()
        assert "T+1" in page.locator("#help-search-results").inner_text()
        page.locator("#help-search-clear").click()
        assert page.locator("#help-search-results").is_hidden()

        page.locator('[data-help-link="calculators"]').click()
        page.locator("#calc-compound").wait_for(state="visible")
        assert page.locator('#calc-compound [data-output="annual"]').inner_text() == "10.00%"

        page.locator('[data-help-link="models"]').click()
        page.locator('#help-models [data-help-tab="decision"]').click()
        page.locator("#tab-decision").wait_for(state="visible")
        assert page.evaluate("window.__helpObservers.disconnected") == 1

        help_button.click()
        page.locator("#tab-help").wait_for(state="visible")
        assert page.evaluate("window.__helpObservers.created") == 2
        for width, height in ((1360, 900), (900, 900), (390, 844)):
            page.set_viewport_size({"width": width, "height": height})
            _wait_for_document_fit(page)
        assert page.locator(".help-mobile-toc").is_visible()
        browser.close()


def test_decision_pick_expands_inline_and_toggles_asset_lists(live_server):
    url, _ = live_server
    decision = {
        "market": {
            "current": {
                "as_of": "2026-07-27",
                "universe_size": 2,
                "state_label": "震荡",
                "bull_score": 52,
                "trend_score": 0.05,
                "advance_ratio": 0.5,
                "above_ma20_ratio": 0.55,
                "macd_hist": 0.01,
                "amount_ratio": 1.1,
                "volatility_20d": 0.02,
            },
            "forecast_validation": [],
            "future": [],
            "sectors": [],
            "past": [],
        },
        "selection": {
            "recommended_exposure": 0.5,
            "holding_horizon_days": 3,
            "signal_date": "2026-07-27",
            "risk_note": "测试风险说明",
            "picks": [
                {
                    "rank": 1,
                    "symbol": "600519.SH",
                    "name": "贵州茅台",
                    "industry": "白酒",
                    "score": 82,
                    "action": "buy",
                    "last_close": 1500,
                    "money_ratio": 1.2,
                    "expected_return": 0.03,
                    "stop_loss": 0.04,
                    "take_profit": 0.08,
                    "reasons": ["趋势向上"],
                },
                {
                    "rank": 2,
                    "symbol": "300750.SZ",
                    "name": "宁德时代",
                    "industry": "电池",
                    "score": 76,
                    "action": "buy",
                    "last_close": 260,
                    "money_ratio": 1.1,
                    "expected_return": 0.02,
                    "stop_loss": 0.04,
                    "take_profit": 0.07,
                    "reasons": ["资金改善"],
                },
            ],
        },
        "history": [
            {
                "signal_date": "2026-07-24",
                "holding_horizon_days": 3,
                "profile": "risk_adjusted",
                "recommended_exposure": 0.5,
                "picks": [
                    {"rank": 1, "symbol": "600519.SH", "name": "贵州茅台"},
                    {"rank": 2, "symbol": "300750.SZ", "name": "宁德时代"},
                    {"rank": 3, "symbol": "000858.SZ", "name": "五粮液"},
                ],
                "follow_up_validation": {
                    "status": "in_progress",
                    "horizon_days": 3,
                    "completed_sessions": 2,
                    "available_picks": 3,
                    "average_return": 0.018,
                    "entry_date": "2026-07-25",
                    "evaluation_date": "2026-07-27",
                    "picks": [
                        {
                            "symbol": "600519.SH",
                            "status": "ready",
                            "entry_price": 1500,
                            "price": 1530,
                            "price_date": "2026-07-27",
                            "return": 0.02,
                        },
                        {
                            "symbol": "300750.SZ",
                            "status": "ready",
                            "entry_price": 260,
                            "price": 265.2,
                            "price_date": "2026-07-27",
                            "return": 0.02,
                        },
                        {
                            "symbol": "000858.SZ",
                            "status": "ready",
                            "entry_price": 120,
                            "price": 121.68,
                            "price_date": "2026-07-27",
                            "return": 0.014,
                        },
                    ],
                },
            }
        ],
    }
    lists = {
        "favorites": [{"symbol": "600519.SH", "name": "贵州茅台"}],
        "following": [],
        "holdings": [],
    }
    history_calls = []

    def history_handler(route):
        request_url = route.request.url
        history_calls.append(request_url)
        if "frequency=1m" in request_url:
            route.fulfill(status=404, json={"detail": "1 分钟行情暂不可用"})
            return
        symbol = request_url.split("/api/v1/market/history/", 1)[1].split("?", 1)[0]
        frequency = request_url.split("frequency=", 1)[1].split("&", 1)[0]
        route.fulfill(
            json={
                "symbol": symbol,
                "frequency": frequency,
                "kline": [
                    ["2026-07-24", 10, 10.5, 9.8, 10.8, 1000],
                    ["2026-07-25", 10.5, 11, 10.2, 11.2, 1200],
                ],
            }
        )

    def asset_handler(route):
        request = route.request
        tail = request.url.split("/api/v1/portfolio/lists", 1)[1].split("?", 1)[0].strip("/")
        if request.method == "POST":
            list_name = tail.split("/", 1)[0]
            item = request.post_data_json
            lists[list_name] = [
                existing for existing in lists[list_name] if existing["symbol"] != item["symbol"]
            ]
            lists[list_name].insert(0, item)
        elif request.method == "DELETE":
            list_name, symbol = tail.split("/", 1)
            lists[list_name] = [item for item in lists[list_name] if item["symbol"] != symbol]
        route.fulfill(json=lists)

    empty_market = {"groups": {}}
    with playwright_sync.sync_playwright() as manager:
        browser = manager.chromium.launch()
        page = browser.new_page(viewport={"width": 1280, "height": 900})
        page.route(
            "**/api/v1/market/overview",
            lambda route: route.fulfill(status=200, json=empty_market),
        )
        page.route("**/api/v1/market/history/**", history_handler)
        page.route("**/api/v1/portfolio/lists**", asset_handler)
        page.goto(url)
        page.get_by_role("tab", name="决策", exact=True).click()
        page.wait_for_url(re.compile(r"#today/decision$"))
        page.wait_for_function("() => typeof window.mkChart === 'function'")
        page.evaluate(
            """data => {
              renderDecision(data);
            }""",
            decision,
        )

        history_row = page.locator(".snapshot-record-row").first
        history_detail = page.locator(".snapshot-detail-row").first
        history_box = history_row.bounding_box()
        assert history_box is not None and history_box["height"] <= 56
        playwright_sync.expect(history_row.locator(".snapshot-pick-summary")).to_contain_text("贵州茅台")
        playwright_sync.expect(history_row.locator(".snapshot-summary-validation")).to_contain_text("验证中")
        assert history_detail.is_hidden()
        history_row.click()
        playwright_sync.expect(history_detail).to_be_visible()
        assert history_row.get_attribute("aria-expanded") == "true"
        assert history_detail.locator(".snapshot-pick").count() == 3
        assert history_detail.locator(".snapshot-result-row").count() == 3
        history_row.click()
        playwright_sync.expect(history_detail).to_be_hidden()
        assert history_row.get_attribute("aria-expanded") == "false"

        first_row = page.locator('tr[data-symbol="600519.SH"]')
        first_trigger = first_row.locator("[data-decision-kline-trigger]")
        first_trigger.click()
        page.locator("#decision-kline canvas").wait_for()
        assert page.locator("#tab-decision").is_visible()
        assert not page.locator("#tab-market").is_visible()
        assert page.locator(".decision-detail-row").count() == 1
        assert first_row.evaluate("row => row.nextElementSibling.classList.contains('decision-detail-row')")
        assert first_trigger.get_attribute("aria-expanded") == "true"
        assert page.locator('[data-decision-asset-toggle="favorites"]').inner_text() == "已自选"

        page.locator('[data-decision-frequency="60m"]').click()
        page.locator("#decision-kline canvas").wait_for()
        assert any("frequency=60m" in request_url for request_url in history_calls)

        following = page.locator('[data-decision-asset-toggle="following"]')
        following.click()
        playwright_sync.expect(following).to_have_text("已关注")
        assert first_trigger.get_attribute("aria-expanded") == "true"
        following.click()
        playwright_sync.expect(following).to_have_text("加入关注")
        assert page.locator(".decision-detail-row").count() == 1

        second_trigger = page.locator('tr[data-symbol="300750.SZ"] [data-decision-kline-trigger]')
        second_trigger.click()
        page.locator("#decision-kline canvas").wait_for()
        assert page.locator(".decision-detail-row").count() == 1
        assert first_trigger.get_attribute("aria-expanded") == "false"
        assert second_trigger.get_attribute("aria-expanded") == "true"
        second_trigger.click()
        assert page.locator(".decision-detail-row").count() == 0

        first_trigger.focus()
        page.keyboard.press("Enter")
        page.locator("#decision-kline canvas").wait_for()
        zoom_equivalent_widths = (1600, 1422, 1280, 1164, 1024, 853, 731, 640, 390)
        for viewport_width in zoom_equivalent_widths:
            if page.locator(".decision-detail-row").count():
                first_trigger.evaluate("button => button.click()")
            page.set_viewport_size({"width": viewport_width, "height": 844})
            scroller = page.locator(".decision-table-scroll")
            collapsed_scroll_width = scroller.evaluate("element => element.scrollWidth")
            expected_scroll_left = scroller.evaluate(
                """element => {
                  element.scrollLeft = Math.min(80, element.scrollWidth - element.clientWidth);
                  return element.scrollLeft;
                }"""
            )
            first_trigger.evaluate("button => button.click()")
            page.locator("#decision-kline canvas").wait_for()
            page.evaluate(
                "() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve)))"
            )
            metrics = page.evaluate(
                """() => {
                  const scroller = document.querySelector('.decision-table-scroll');
                      const shell = document.querySelector('.decision-detail-shell');
                      const chart = document.querySelector('.decision-kline-canvas');
                      const scrollRect = scroller.getBoundingClientRect();
                      const shellRect = shell.getBoundingClientRect();
                      const chartRect = chart.getBoundingClientRect();
                      return {
                    clientWidth: scroller.clientWidth,
                    scrollWidth: scroller.scrollWidth,
                    scrollLeft: scroller.scrollLeft,
                    scrollRight: scrollRect.right,
                    shellLeft: shellRect.left,
                    shellRight: shellRect.right,
                        shellWidth: shellRect.width,
                        chartRight: chartRect.right,
                        documentFits: document.documentElement.scrollWidth <= window.innerWidth,
                      };
                }"""
            )
            assert metrics["shellWidth"] <= metrics["clientWidth"] + 1, metrics
            assert metrics["shellLeft"] >= scroller.bounding_box()["x"] - 1, metrics
            assert metrics["shellRight"] <= metrics["scrollRight"] + 1, metrics
            assert metrics["chartRight"] <= metrics["scrollRight"] + 1, metrics
            assert metrics["scrollWidth"] <= collapsed_scroll_width + 1, metrics
            assert abs(metrics["scrollLeft"] - expected_scroll_left) <= 1, metrics
            assert metrics["documentFits"], metrics

        page.locator('[data-decision-frequency="1m"]').click()
        page.locator(".decision-kline-error").wait_for()
        assert "1 分钟行情暂不可用" in page.locator(".decision-kline-error").inner_text()
        assert page.locator("#tab-decision").is_visible()
        browser.close()


def _legacy_kline_cache_and_stale_view_protection(live_server):
    url, _ = live_server
    empty_market = {"groups": {}}
    with playwright_sync.sync_playwright() as manager:
        browser = manager.chromium.launch()
        page = browser.new_page(viewport={"width": 1280, "height": 900})
        page.route(
            "**/api/v1/market/overview",
            lambda route: route.fulfill(status=200, json=empty_market),
        )
        page.goto(url)
        page.evaluate(
            "() => import('/static/advanced-charts.js').then(module => module.loadAdvancedCharts())"
        )
        result = page.evaluate(
            """async () => {
              const originalApi = api;
              const originalLoadKlineSeries = loadKlineSeries;
              const originalNow = Date.now;
              const originalMarketLoading = marketLoading;
              const networkCalls = [];
              const bar = symbol => ({
                symbol, frequency:'1d',
                kline:[
                  ['2026-08-07',10,10.2,9.8,10.3,1000],
                  ['2026-08-08',10.2,10.4,10.1,10.5,1100],
                ],
              });
              let now = 1_800_000_000_000;
              try {
                Date.now = () => now;
                api = async path => {
                  networkCalls.push(path);
                  return bar(path.includes('60m') ? 'MINUTE.SH' : 'CACHE.SH');
                };

                invalidateKlineSeriesCache();
                await loadKlineSeries('CACHE.SH','1d');
                await loadKlineSeries('CACHE.SH','1d');
                const cachedDailyCalls = networkCalls.length;
                now += KLINE_DAILY_TTL_MS - 1;
                await loadKlineSeries('CACHE.SH','1d');
                const beforeDailyExpiry = networkCalls.length;
                now += 2;
                await loadKlineSeries('CACHE.SH','1d');
                const afterDailyExpiry = networkCalls.length;

                invalidateKlineSeriesCache();
                await loadKlineSeries('MINUTE.SH','60m');
                now += KLINE_INTRADAY_TTL_MS - 1;
                await loadKlineSeries('MINUTE.SH','60m');
                const beforeMinuteExpiry = networkCalls.length;
                now += 2;
                await loadKlineSeries('MINUTE.SH','60m');
                const afterMinuteExpiry = networkCalls.length;
                invalidateKlineSeriesCache();
                await loadKlineSeries('MINUTE.SH','60m');
                const afterManualInvalidation = networkCalls.length;

                invalidateKlineSeriesCache();
                for (let index = 0; index < 65; index += 1) {
                  await loadKlineSeries(`LRU${index}.SH`,'1d');
                }
                const lruSize = klineSeriesCache.size;
                const oldestEvicted = !Array.from(klineSeriesCache.keys()).some(
                  key => key.startsWith('LRU0.SH\u0000'),
                );

                invalidateKlineSeriesCache();
                let finishShared;
                let sharedNetworkSignal;
                const sharedStart = networkCalls.length;
                api = (path, options) => {
                  networkCalls.push(path);
                  sharedNetworkSignal = options.signal;
                  return new Promise(resolve => {
                    finishShared = () => resolve(bar('SHARED.SH'));
                  });
                };
                const firstConsumer = new AbortController();
                const secondConsumer = new AbortController();
                const firstShared = loadKlineSeries(
                  'SHARED.SH','1d',{signal:firstConsumer.signal},
                ).catch(error => error.name);
                const secondShared = loadKlineSeries(
                  'SHARED.SH','1d',{signal:secondConsumer.signal},
                );
                firstConsumer.abort();
                const sharedStayedAlive = !sharedNetworkSignal.aborted;
                finishShared();
                const firstSharedState = await firstShared;
                const secondSharedData = await secondShared;
                const sharedNetworkCalls = networkCalls.length - sharedStart;

                Date.now = originalNow;
                const pendingViews = {};
                const waitForPending = async (symbol, promise) => {
                  let outcome = 'pending';
                  promise.then(
                    () => { outcome = 'resolved'; },
                    error => { outcome = `rejected: ${error?.message || error}`; },
                  );
                  for (let attempt = 0; attempt < 50; attempt += 1) {
                    if (pendingViews[symbol]) return;
                    await new Promise(resolve => setTimeout(resolve, 0));
                  }
                  throw new Error(`${symbol} did not reach loadKlineSeries (${outcome})`);
                };
                loadKlineSeries = (symbol, frequency, {signal} = {}) =>
                  new Promise(resolve => {
                    pendingViews[symbol] = {resolve,signal};
                  });
                marketLoading = true;
                const startedAt = performance.now();
                const firstView = showKline('FIRST.SH','旧标的');
                await waitForPending('FIRST.SH', firstView);
                const showLatencyMs = performance.now() - startedAt;
                const panel = document.getElementById('kline-panel');
                const panelVisibleImmediately = getComputedStyle(panel).display !== 'none';
                const panelTop = panel.getBoundingClientRect().top;
                const secondView = showKline('SECOND.SH','新标的');
                await waitForPending('SECOND.SH', secondView);
                const firstSignalAborted = pendingViews['FIRST.SH'].signal.aborted;
                pendingViews['SECOND.SH'].resolve(bar('SECOND.SH'));
                await secondView;
                pendingViews['FIRST.SH'].resolve(bar('FIRST.SH'));
                await firstView;
                const renderedSymbol = charts.kline.__quantmasterKlineData.symbol;

                return {
                  cachedDailyCalls, beforeDailyExpiry, afterDailyExpiry,
                  beforeMinuteExpiry, afterMinuteExpiry, afterManualInvalidation,
                  lruSize, oldestEvicted, sharedStayedAlive, firstSharedState,
                  secondSharedSymbol:secondSharedData.symbol, sharedNetworkCalls,
                  showLatencyMs, panelVisibleImmediately, panelTop,
                  viewportHeight:window.innerHeight,
                  firstSignalAborted, renderedSymbol,
                  title:document.getElementById('kline-title').textContent,
                };
              } finally {
                Date.now = originalNow;
                api = originalApi;
                loadKlineSeries = originalLoadKlineSeries;
                marketLoading = originalMarketLoading;
                invalidateKlineSeriesCache();
              }
            }"""
        )

        assert result["cachedDailyCalls"] == 1
        assert result["beforeDailyExpiry"] == 1
        assert result["afterDailyExpiry"] == 2
        assert result["afterMinuteExpiry"] == result["beforeMinuteExpiry"] + 1
        assert result["afterManualInvalidation"] == result["afterMinuteExpiry"] + 1
        assert result["lruSize"] == 64
        assert result["oldestEvicted"] is True
        assert result["sharedNetworkCalls"] == 1
        assert result["sharedStayedAlive"] is True
        assert result["firstSharedState"] == "AbortError"
        assert result["secondSharedSymbol"] == "SHARED.SH"
        assert result["showLatencyMs"] < 100
        assert result["panelVisibleImmediately"] is True
        assert 0 <= result["panelTop"] < result["viewportHeight"] / 2
        assert result["firstSignalAborted"] is True
        assert result["renderedSymbol"] == "SECOND.SH"
        assert result["title"] == "新标的（SECOND.SH）· 日线"
        browser.close()


def _legacy_major_indexes_are_first_and_personal_group_shows_memberships(live_server):
    url, _ = live_server
    personal = {
        "symbol": "600519.SH",
        "name": "贵州茅台",
        "last": 1530.0,
        "change_pct": 0.99,
        "nav": [[1784505600000, 1.0], [1784592000000, 1.02]],
        "as_of": "2026-07-21",
        "cache_status": "ready",
        "memberships": ["favorites", "holdings"],
        "rsi_14": 58.0,
        "rsi_history": [
            ["2026-05-01", 40.0],
            ["2026-06-15", 52.0],
            ["2026-07-21", 58.0],
        ],
    }
    index = {
        "symbol": "000300.SH",
        "name": "沪深300",
        "last": 4600.0,
        "change_pct": -0.2,
        "nav": [[1784505600000, 1.0], [1784592000000, 0.998]],
        "as_of": "2026-07-21",
        "cache_status": "ready",
        "rsi_14": 60.0,
        "rsi_history": [
            ["2026-03-01", 20.0],
            ["2026-04-20", 30.0],
            ["2026-05-01", 35.0],
            ["2026-06-15", 50.0],
            ["2026-07-21", 60.0],
        ],
    }
    market_snapshot = {"groups": {"我的股票": [personal], "A股指数": [index]}}
    kline = [
        [f"2026-06-{day:02d}", price, price + 0.2, price - 0.3, price + 0.5, 1000 + day]
        for day, price in enumerate([100.0] + [10.0 + index * 0.1 for index in range(27)], 1)
    ]
    history_calls = []

    def history_handler(route):
        history_calls.append(route.request.url)
        route.fulfill(
            json={
                "symbol": "000300.SH",
                "frequency": "1d",
                "kline": kline,
            }
        )

    with playwright_sync.sync_playwright() as manager:
        browser = manager.chromium.launch()
        page = browser.new_page(viewport={"width": 1280, "height": 900})
        page.route(
            "**/api/v1/market/overview",
            lambda route: route.fulfill(status=200, json=market_snapshot),
        )
        page.route(
            "**/api/v1/market/history/**",
            history_handler,
        )
        page.goto(url)
        personal_section = page.locator('[data-market-group="我的股票"]')
        index_section = page.locator('[data-market-group="A股指数"]')
        personal_section.wait_for()
        index_section.wait_for()

        assert personal_section.locator(".market-section-title").inner_text() == "我的股票"
        assert index_section.locator(".market-section-title").inner_text() == "主要指数"
        assert personal_section.locator(".mkt-memberships").inner_text() == "自选 · 持有"
        assert "贵州茅台" in personal_section.locator(".nm").inner_text()
        assert "600519.SH" in personal_section.locator(".nm").inner_text()
        assert personal_section.locator(".mkt-item").count() == 1
        assert index_section.locator(".mkt-item").count() == 1
        assert index_section.locator("canvas").is_visible()
        assert index_section.locator(".spark").bounding_box()["height"] >= 70
        assert "区间 -0.20%" in index_section.locator(".mkt-spark-foot").inner_text()
        assert "07.20—07.21" in index_section.locator(".mkt-spark-period").inner_text()
        price_spark = index_section.locator(".spark canvas")
        assert price_spark.get_attribute("data-native-chart") == "market-spark"
        assert "最新区间涨跌 -0.20%" in index_section.locator(".spark").get_attribute("aria-label")
        price_spark.scroll_into_view_if_needed()
        spark_bounds = price_spark.bounding_box()
        page.mouse.move(
            spark_bounds["x"] + spark_bounds["width"] * 0.75,
            spark_bounds["y"] + spark_bounds["height"] / 2,
        )
        price_tooltip = index_section.locator(".native-chart-tooltip")
        price_tooltip.wait_for(state="visible")
        assert "区间涨跌" in price_tooltip.inner_text()
        assert "当日涨跌" in price_tooltip.inner_text()
        assert page.evaluate(
            "history => rsiSparkPoints(history).map(point => point.date)",
            index["rsi_history"],
        ) == ["2026-05-01", "2026-06-15", "2026-07-21"]
        rsi_spark = index_section.locator(".mkt-rsi-spark")
        rsi_bounds = rsi_spark.bounding_box()
        page.mouse.move(
            rsi_bounds["x"] + rsi_bounds["width"] / 2,
            rsi_bounds["y"] + 18,
        )
        tooltip = index_section.locator(".mkt-rsi-tooltip")
        tooltip.wait_for(state="visible")
        assert tooltip.locator("[data-rsi-hover-date]").inner_text() == "2026-06-15"
        assert tooltip.locator("[data-rsi-hover-value]").inner_text() == "RSI 50.0"
        hover_dot = index_section.locator(".mkt-rsi-hover-dot")
        assert hover_dot.get_attribute("visibility") == "visible"
        hover_dot_bounds = hover_dot.bounding_box()
        assert abs(hover_dot_bounds["width"] - hover_dot_bounds["height"]) <= 0.5
        assert max(hover_dot_bounds["width"], hover_dot_bounds["height"]) <= 4.0
        page.mouse.move(rsi_bounds["x"] - 4, rsi_bounds["y"] + 18)
        tooltip.wait_for(state="hidden")
        index_section.locator(".mkt-item").click()
        page.locator("#kline canvas").wait_for()
        assert len(history_calls) == 1
        assert "start=2023-01-01" not in history_calls[0]
        panel_top = page.locator("#kline-panel").evaluate(
            "element => element.getBoundingClientRect().top",
        )
        assert 0 <= panel_top < page.viewport_size["height"] / 2
        index_section.locator(".mkt-item").click()
        page.wait_for_timeout(50)
        assert len(history_calls) == 1
        zoom_snapshot = """() => {
          const chart = charts.kline;
          const option = chart.getOption();
          const zoom = id => {
            const item = option.dataZoom.find(candidate => candidate.id === id);
            return {start:Number(item.start),end:Number(item.end),filterMode:item.filterMode};
          };
          return {
            x:zoom('market-kline-x-wheel'),
            y:zoom('qm-zoom-y-wheel'),
            yExtent:chart.getModel().getComponent('yAxis',0).axis.scale.getExtent(),
          };
        }"""
        before_wheel = page.evaluate(zoom_snapshot)
        assert before_wheel["x"]["filterMode"] == "none"
        page.locator("#kline").evaluate(
            """element => {
              const bounds = element.getBoundingClientRect();
              element.dispatchEvent(new WheelEvent('wheel',{
                deltaX:-120,deltaY:0,ctrlKey:true,bubbles:true,cancelable:true,
                clientX:bounds.left + bounds.width / 2,
                clientY:bounds.top + bounds.height / 2,
              }));
            }"""
        )
        page.wait_for_timeout(50)
        after_wheel = page.evaluate(zoom_snapshot)
        assert after_wheel["x"]["start"] > before_wheel["x"]["start"]
        assert after_wheel["x"]["end"] < before_wheel["x"]["end"]
        assert after_wheel["y"] == before_wheel["y"]
        assert after_wheel["yExtent"] == before_wheel["yExtent"]
        assert index_section.bounding_box()["y"] < personal_section.bounding_box()["y"]

        page.set_viewport_size({"width": 390, "height": 844})
        _wait_for_document_fit(page)
        assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
        browser.close()


def test_backtest_factor_completion_supports_lab_names_and_comma_segments(live_server):
    url, _ = live_server
    factors = [
        {
            "name": "mom_20d",
            "description": "20 日动量",
            "source": "builtin",
        },
        {
            "name": "人工反转",
            "slug": "manual_a1b2c3d4e5",
            "description": "Quant Lab 人工表达式",
            "category": "人工研究",
            "status": "candidate",
            "source": "quant_lab",
        },
        {
            "name": "GP 候选 2",
            "slug": "gp_2222222222",
            "description": "遗传规划候选",
            "category": "AI 发现",
            "status": "draft",
            "source": "quant_lab",
        },
    ]
    with playwright_sync.sync_playwright() as manager:
        browser = manager.chromium.launch()
        page = browser.new_page(viewport={"width": 1280, "height": 900})
        page.route("**/api/v1/research/factors", lambda route: route.fulfill(json={"factors": factors}))
        page.goto(url)
        page.get_by_role("button", name="研究", exact=True).click()
        page.get_by_role("tab", name="回测", exact=True).click()
        page.locator('#bt-form [name="strategy"]').select_option("factor")
        factor_input = page.locator("#bt-factor-input")
        menu = page.locator("#bt-factor-options")
        trigger = page.locator(".factor-completion-trigger")
        trigger.click()
        menu.wait_for(state="visible")
        assert factor_input.get_attribute("aria-expanded") == "true"
        menu.locator('[role="option"]', has_text="mom_20d").wait_for(state="visible")
        factor_input.press("Escape")
        menu.wait_for(state="hidden")
        assert factor_input.get_attribute("aria-expanded") == "false"

        factor_input.fill("mom_20d,")
        menu.wait_for(state="visible")
        assert factor_input.get_attribute("aria-expanded") == "true"
        assert "人工反转" in menu.inner_text()
        assert "GP 候选 2" in menu.inner_text()
        assert "mom_20d" not in menu.inner_text()
        input_box = factor_input.bounding_box()
        menu_box = menu.bounding_box()
        assert (
            menu_box["y"] >= input_box["y"] + input_box["height"]
            or menu_box["y"] + menu_box["height"] <= input_box["y"]
        )

        factor_input.type("人工")
        assert menu.locator('[role="option"]').count() == 1
        assert "Quant Lab 人工表达式" in menu.inner_text()
        factor_input.press("ArrowDown")
        factor_input.press("Enter")
        assert factor_input.input_value() == "mom_20d, 人工反转"
        assert factor_input.get_attribute("aria-expanded") == "false"

        factor_input.type(",")
        menu.wait_for(state="visible")
        assert "GP 候选 2" in menu.inner_text()
        assert "人工反转" not in menu.inner_text()
        menu.locator('[role="option"]', has_text="GP 候选 2").click()
        assert factor_input.input_value() == "mom_20d, 人工反转, GP 候选 2"
        menu.wait_for(state="hidden")
        assert factor_input.get_attribute("aria-expanded") == "false"
        browser.close()


def test_backtest_workspace_and_history_keep_a_clear_responsive_order(live_server):
    url, _ = live_server
    with playwright_sync.sync_playwright() as manager:
        browser = manager.chromium.launch()
        page = browser.new_page(viewport={"width": 1280, "height": 900})
        page.goto(url)
        page.get_by_role("button", name="研究", exact=True).click()
        page.evaluate(
            """() => { window.__backtestMounted = new Promise(resolve => {
              document.addEventListener('quantmaster:workspace-mounted', event => {
                if (event.detail?.workspace === 'research' && event.detail?.page === 'backtest') resolve();
              });
            }); }"""
        )
        page.get_by_role("tab", name="回测", exact=True).click()
        page.evaluate("window.__backtestMounted")
        page.locator("#tab-backtest").evaluate(
            "element => Promise.all(element.getAnimations().map(animation => animation.finished))"
        )

        config = page.locator("#tab-backtest .trading-config")
        workspace = page.locator("#tab-backtest .trading-workspace")
        history = page.locator("#tab-backtest .trading-history-section")
        headers = history.get_by_role("columnheader")
        playwright_sync.expect(headers).to_have_count(6)
        assert headers.all_inner_texts() == [
            "选择", "实验名称", "候选 · 策略", "年化收益", "状态", "操作",
        ]
        assert all(header.is_visible() for header in headers.all())

        desktop = [locator.bounding_box() for locator in (config, workspace, history)]
        assert abs(desktop[0]["y"] - desktop[1]["y"]) < 1
        assert history.evaluate("element => getComputedStyle(element).gridArea") == "history"
        assert desktop[2]["y"] >= max(
            desktop[0]["y"] + desktop[0]["height"],
            desktop[1]["y"] + desktop[1]["height"],
        ) - 1

        page.set_viewport_size({"width": 390, "height": 844})
        _wait_for_document_fit(page)
        mobile = [locator.bounding_box() for locator in (config, workspace, history)]
        assert mobile[0]["y"] < mobile[1]["y"] < mobile[2]["y"]
        assert headers.nth(0).is_visible()
        assert headers.nth(1).is_visible()
        dom_headers = history.locator('.trading-history-columns [role="columnheader"]')
        assert dom_headers.nth(0).is_visible()
        assert dom_headers.nth(1).is_visible()
        assert dom_headers.nth(2).is_hidden()
        assert dom_headers.nth(3).is_hidden()
        assert dom_headers.nth(4).is_visible()
        assert dom_headers.nth(5).is_visible()
        browser.close()


def test_automation_subscriptions_audit_and_source_save_feedback(live_server):
    url, _ = live_server
    target = {
        "id": "feishu_owner",
        "channel": "feishu",
        "label": "飞书管理员私聊",
        "target": "oc_test",
        "account_id": "cli_app",
        "chat_type": "direct",
        "enabled": True,
        "preset": "balanced",
        "overrides": {},
        "status": "healthy",
        "last_error": "",
        "owner_actor": "feishu:cli_app:ou_owner",
        "has_context": False,
        "updated_at": "2026-07-27T10:00:00+00:00",
    }
    job = {
        "name": "news_digest",
        "enabled": False,
        "job_kind": "daily",
        "schedule": {"type": "daily", "times": ["11:35", "21:00"]},
        "args": {},
        "next_run": None,
        "updated_at": "2026-07-27T10:00:00+00:00",
        "execution": {
            "id": "job-1", "active_job_id": "job-1", "status": "running",
            "progress": 1, "phase": "fetch", "detail": "等待 provider",
            "running_instances": 1, "coalesced_count": 2,
            "started_at": "2026-07-27T10:00:00+00:00", "finished_at": "",
            "heartbeat_at": "2026-07-27T10:02:00+00:00",
            "last_completed_unit_at": "2026-07-27T10:01:30+00:00",
            "elapsed_seconds": 120,
            "queue": {"pending": 3, "running": 1, "retry_wait": 1, "dead_letter": 0},
            "backoff": {
                "active": True, "reason": "provider Retry-After",
                "waiting_on": "provider", "next_retry_at": "2026-07-27T10:05:00+00:00",
            },
            "stalled": {
                "is_stalled": False, "reason": "", "diagnostic_code": "",
                "observed_at": "", "waiting_on": "",
            },
            "links": {"self": "/api/v1/jobs/job-1"},
        },
    }
    overview = {
        "enabled": True,
        "timezone": "Asia/Shanghai",
        "runtime": {"status": "running", "worker": {"phase": "serving"}},
        "bot_accounts": [
            {
                "channel": "feishu",
                "account_id": "cli_app",
                "status": "listening",
                "last_error": "",
            }
        ],
        "jobs": [job],
        "recent_events": [],
        "targets": [target],
        "queue_summary": {
            "queued": 0, "running": 1, "retry_wait": 1, "failed": 0,
            "dead_letter": 0, "coalesced_count": 2,
        },
        "outbox": {
            "dispatcher_status": "running", "pending": 2, "leased": 0,
            "retry_wait": 1, "sent": 4, "dead_letter": 0,
            "next_retry_at": "2026-07-27T10:05:00+00:00",
        },
        "inbound": {
            "feishu": {
                "total": 1,
                "last_received_at": "2026-07-27T10:02:00+00:00",
                "direct": {"total": 1, "last_received_at": "2026-07-27T10:02:00+00:00"},
                "group": {"total": 0, "last_received_at": ""},
            },
            "weixin": {"total": 0, "last_received_at": ""},
        },
    }
    source = {
        "id": "sse",
        "name": "上海证券交易所",
        "kind": "builtin",
        "group_name": "official",
        "url": "https://www.sse.com.cn/",
        "item_limit": 30,
        "factor_weight": 1,
        "enabled": True,
        "is_official": True,
        "built_in": True,
        "auth_type": "none",
        "auth_header": "",
        "auth_configured": False,
        "parser": {},
        "last_error": "",
        "last_run": "",
    }
    requests = {"audit": 0, "events": 0, "jobs": 0, "run": 0}

    def policy_handler(route):
        body = route.request.post_data_json
        target["preset"] = body["preset"]
        target["overrides"] = body.get("overrides") or {}
        if body.get("enabled") is not None:
            target["enabled"] = body["enabled"]
        route.fulfill(status=200, json=target)

    def audit_handler(route):
        requests["audit"] += 1
        route.fulfill(
            status=200,
            json={
                "items": [
                    {
                        "created_at": "2026-07-27T10:00:00+00:00",
                        "actor": "web",
                        "action": "update_policy",
                        "object_type": "target",
                        "object_id": "feishu_owner",
                        "result": "ok",
                    }
                ]
            },
        )

    def event_handler(route):
        requests["events"] += 1
        route.fulfill(
            status=200,
            json={
                "items": [
                    {
                        "id": "event-1",
                        "kind": "important_news",
                        "score": 82,
                        "direction": "up",
                        "occurred_at": "2026-07-27T10:03:00+00:00",
                        "payload": {"title": "测试市场事件"},
                    }
                ]
            },
        )

    def jobs_handler(route):
        if route.request.url.endswith("/run"):
            requests["run"] += 1
            route.fulfill(
                status=200,
                json={
                    "status": "accepted",
                    "run_id": "job-2",
                    "job_id": "job-2",
                    "task": "news_digest",
                    "created": False,
                    "progress": 1,
                },
            )
            return
        if route.request.method == "PATCH":
            job["enabled"] = route.request.post_data_json["action"] == "resume"
            route.fulfill(status=200, json=job)
            return
        requests["jobs"] += 1
        route.fulfill(
            status=200,
            json={
                "jobs": [job],
                "runs": [
                    {
                        "domain": "automation",
                        "id": "job-1",
                        "type": "automation.news_digest",
                        "status": "completed",
                        "progress": 1,
                        "phase": "completed",
                        "detail": "摘要生成完成",
                        "attempt": 1,
                        "cancel_requested": False,
                        "created_at": "2026-07-27T09:59:00+00:00",
                        "updated_at": "2026-07-27T10:01:00+00:00",
                        "estimated_remaining_seconds": 0,
                        "can_cancel": False,
                        "can_retry": False,
                        "links": {},
                    }
                ],
            },
        )

    def source_handler(route):
        if route.request.method == "GET":
            route.fulfill(status=200, json={"items": [source]})
        else:
            route.fulfill(status=200, json=source)

    with playwright_sync.sync_playwright() as manager:
        browser = manager.chromium.launch()
        page = browser.new_page(viewport={"width": 390, "height": 844})
        dialogs = []

        def dismiss_dialog(dialog):
            dialogs.append(dialog.message)
            dialog.dismiss()

        page.on("dialog", dismiss_dialog)
        page.route("**/api/v1/automation/overview", lambda route: route.fulfill(json=overview))
        page.route("**/api/v1/automation/audit*", audit_handler)
        page.route("**/api/v1/automation/events*", event_handler)
        page.route("**/api/v1/automation/jobs", jobs_handler)
        page.route("**/api/v1/automation/jobs/**", jobs_handler)
        page.route("**/api/v1/automation/targets/*/policy", policy_handler)
        page.route("**/api/v1/news/sources*", source_handler)
        page.goto(url)

        page.get_by_role("button", name="运行", exact=True).click()
        page.locator("#automation-overview").get_by_text(
            "重要消息摘要已暂停",
            exact=True,
        ).wait_for()
        overview_tab = page.get_by_role("tab", name="运行总览", exact=True)
        overview_tab.focus()
        overview_tab.press("ArrowRight")
        assert page.get_by_role("tab", name="任务调度", exact=True).get_attribute("aria-selected") == "true"

        page.locator('[data-job-row="news_digest"] [data-job-expand]').click()
        assert page.locator('[data-job-row="news_digest"] progress').get_attribute("value") == "1"
        assert "高频" not in page.locator('[data-job-row="news_digest"]').inner_text()
        assert "每日业务" in page.locator('[data-job-row="news_digest"]').inner_text()
        assert "待处理 3 · 运行 1 · 重试 1 · 死信 0" in page.locator(
            '[data-job-row="news_digest"]'
        ).inner_text()
        assert "合法退避" in page.locator('[data-job-row="news_digest"]').inner_text()
        page.get_by_role("button", name="立即运行任务", exact=True).click()
        _wait_for_class(
            page.locator(
                '[data-job-row="news_digest"] .automation-row-feedback',
            ),
            "success",
        )
        assert requests["run"] == 1
        assert "已连接同一任务 job-2 · 当前 1%" in page.locator(
            '[data-job-row="news_digest"] .automation-row-feedback'
        ).inner_text()

        page.get_by_role("tab", name="消息推送", exact=True).click()
        page.locator('[data-target-card="feishu_owner"]').wait_for()
        page.locator('[data-target-card="feishu_owner"] [data-target-expand]').click()
        assert requests["audit"] == 0
        assert requests["events"] == 0
        assert requests["jobs"] == 0

        for kind in ("important_news", "market_turn", "market_close", "task_report", "task_failure"):
            page.locator(f'[data-target="feishu_owner"][data-event-type="{kind}"]').uncheck()
            _wait_for_class(
                page.locator(
                    '[data-target-card="feishu_owner"] .target-feedback',
                ),
                "success",
            )
        assert target["overrides"]["event_types"] == []
        assert (
            "自动化与 Bot 监听仍会继续运行"
            in page.locator('[data-target-card="feishu_owner"] .target-content-note').inner_text()
        )

        page.get_by_role("tab", name="运行记录", exact=True).click()
        page.locator("#automation-runs").get_by_text("job-1", exact=True).wait_for()
        assert requests["jobs"] == 1
        assert requests["events"] == 0
        assert requests["audit"] == 0

        runs_tab = page.get_by_role("tab", name="任务运行", exact=True)
        runs_tab.focus()
        runs_tab.press("ArrowRight")
        page.get_by_text("测试市场事件", exact=True).wait_for()
        assert requests["events"] == 1
        assert requests["audit"] == 0

        page.get_by_role("tab", name="操作审计", exact=True).click()
        page.get_by_text("update_policy", exact=True).wait_for()
        assert requests["audit"] == 1

        page.get_by_role("button", name="刷新状态", exact=True).click()
        page.wait_for_function(
            "() => document.querySelector('#automation-page-feedback')?.textContent.includes('已刷新')"
        )
        page.wait_for_function(
            "() => document.querySelector('#automation-audit')?.textContent.includes('update_policy')"
        )
        assert requests["audit"] == 2
        assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
        assert dialogs == []

        page.emulate_media(reduced_motion="reduce")
        page.get_by_role("tab", name="任务调度", exact=True).click()
        assert (
            page.locator("#automation-panel-jobs").evaluate(
                "element => getComputedStyle(element).animationName"
            )
            == "none"
        )

        page.set_viewport_size({"width": 1280, "height": 900})
        page.get_by_role("tab", name="消息推送", exact=True).click()
        channel_status = page.locator("#automation-channel-feishu .status-label")
        assert channel_status.evaluate("element => getComputedStyle(element).whiteSpace") == "nowrap"
        assert channel_status.evaluate("element => element.getBoundingClientRect().height") < 24
        target_toggle = page.locator('[data-target-card="feishu_owner"] [data-target-expand]')
        if target_toggle.get_attribute("aria-expanded") != "true":
            target_toggle.click()
        subscription_checkbox = page.locator(
            '[data-target-card="feishu_owner"] .target-content-options input'
        ).first
        assert subscription_checkbox.evaluate("element => element.getBoundingClientRect().width") < 18
        assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")

        page.get_by_role("button", name="设置", exact=True).click()
        page.locator('[data-settings-section="sources"]').click()
        page.locator('[data-source-id="sse"]').click()
        page.locator('#news-source-editor button[type="submit"]').click()
        _wait_for_class(page.locator("#news-source-feedback"), "success")
        assert "已保存" in page.locator("#news-source-feedback").inner_text()
        browser.close()


def _legacy_market_style_confirmation_path_chart_layout(live_server):
    url, _ = live_server
    states = [
        ("weak_rebound", "pending"),
        ("weak_rebound", "pending"),
        ("weak_rebound", "weak_rebound"),
        ("balanced", "pending"),
        ("balanced", "pending"),
        ("balanced", "balanced"),
        ("strong_dominant", "pending"),
        ("strong_dominant", "pending"),
        ("strong_dominant", "strong_dominant"),
        ("strong_dominant", "strong_dominant"),
        ("balanced", "pending"),
        ("balanced", "pending"),
        ("weak_rebound", "pending"),
        ("weak_rebound", "weak_rebound"),
        ("strong_dominant", "pending"),
        ("strong_dominant", "strong_dominant"),
        ("balanced", "pending"),
        ("balanced", "balanced"),
    ]
    history = []
    spread_by_state = {
        "weak_rebound": -0.004,
        "balanced": 0.001,
        "strong_dominant": 0.005,
    }
    for day, (candidate, confirmed) in enumerate(states, 1):
        spread = spread_by_state[candidate]
        history.append(
            {
                "date": f"2026-07-{day:02d}",
                "strong_return": 0.003 + spread / 2,
                "weak_return": 0.003 - spread / 2,
                "spread": spread,
                "candidate": candidate,
                "confirmed": confirmed,
            }
        )
    payload = {
        "meta": {
            "as_of": "2026-07-18",
            "algorithm_version": "QM_ROTATION_V7",
            "sources": ["local:bars"],
            "quality": {"status": "complete", "issues": []},
        },
        "data": {
            "current": {
                "candidate": "balanced",
                "confirmed": "pending",
                "candidate_sessions": 2,
                "confirmed_sessions": 0,
                "spread_1d": 0.001,
                "spread_3d": 0.0023,
            },
            "history": history,
            "distribution": [
                {
                    "state": state,
                    "label": label,
                    "count": count,
                    "share": count / 100,
                    "positive_ratio": positive,
                    "median_return": median,
                }
                for state, label, count, positive, median in [
                    ("strong_up", "强势加速", 18, 0.72, 0.009),
                    ("up", "趋势延续", 34, 0.61, 0.004),
                    ("range", "中位整理", 31, 0.48, -0.001),
                    ("weak", "低位偏弱", 17, 0.35, -0.007),
                ]
            ],
            "leaders": [],
            "laggards": [],
        },
    }

    with playwright_sync.sync_playwright() as manager:
        browser = manager.chromium.launch()
        page = browser.new_page(
            viewport={"width": 1280, "height": 900},
            reduced_motion="reduce",
        )
        page_errors = []
        page.on("pageerror", lambda error: page_errors.append(str(error)))
        page.route(
            "**/api/v1/market/structure",
            lambda route: route.fulfill(json=payload),
        )
        page.goto(f"{url}/#today/style")

        path_chart = page.locator("#rotation-style-path-chart")
        path_chart.locator("canvas").wait_for(state="visible")
        main_chart = page.locator("#rotation-structure-chart")
        color_state = page.evaluate(
            """() => {
              const resolve = variable => {
                const probe = document.createElement('i');
                probe.style.color = `var(${variable})`;
                document.body.append(probe);
                const color = getComputedStyle(probe).color;
                probe.remove();
                return color;
              };
              const current = document.querySelector('.rotation-style-current-kpi');
              const rows = [...document.querySelectorAll(
                '.rotation-style-distribution .rotation-state-row'
              )];
              return {
                style: current.dataset.style,
                confirmation: current.dataset.confirmation,
                currentColor: getComputedStyle(
                  current.querySelector('.rotation-style-current-value')
                ).color,
                pendingColor: getComputedStyle(
                  current.querySelector('.rotation-style-confirmation')
                ).color,
                currentBackground: getComputedStyle(current).backgroundColor,
                pageBackground: getComputedStyle(document.body).backgroundColor,
                rowColors: Object.fromEntries(rows.map(row => [
                  row.dataset.state, getComputedStyle(row.querySelector('strong')).color,
                ])),
                tokens: {
                  up: resolve('--up'),
                  down: resolve('--down'),
                  balanced: resolve('--s1'),
                  pending: resolve('--s4'),
                },
              };
            }"""
        )
        assert color_state["style"] == "balanced"
        assert color_state["confirmation"] == "pending"
        assert color_state["currentColor"] == color_state["tokens"]["balanced"]
        assert color_state["pendingColor"] == color_state["tokens"]["pending"]
        assert color_state["currentBackground"] != color_state["pageBackground"]
        assert color_state["rowColors"]["strong_up"] == color_state["tokens"]["up"]
        assert color_state["rowColors"]["range"] == color_state["tokens"]["balanced"]
        assert color_state["rowColors"]["weak"] == color_state["tokens"]["down"]
        assert len(set(color_state["rowColors"].values())) == 4
        chart_colors = page.evaluate(
            """() => {
              const structure = charts['rotation-structure-chart'].__qmLastOption;
              const path = charts['rotation-style-path-chart'].__qmLastOption;
              const strong = structure.series.find(series => series.name === '强势样本');
              const weak = structure.series.find(series => series.name === '低位样本');
              const spread = structure.series.find(series => series.name === '强弱差');
              return {
                strongBar: {
                  type: strong.type, color: strong.itemStyle.color,
                  plotted: strong.data[0].value[1], raw: strong.data[0].rawReturn,
                },
                weakBar: {
                  type: weak.type, color: weak.itemStyle.color,
                  plotted: weak.data[0].value[1], raw: weak.data[0].rawReturn,
                },
                spreadLine: {
                  type: spread.type, color: spread.lineStyle.color,
                  lineType: spread.lineStyle.type, showSymbol: spread.showSymbol,
                },
                axisExtent: [structure.yAxis.min,structure.yAxis.max],
                tooltipText: structure.tooltip.formatter([{
                  seriesName: weak.name, axisValue: weak.data[0].value[0],
                  data: weak.data[0], value: weak.data[0].value, marker: '',
                }]),
                deadZone: spread.markArea.data[0].map(point => point.yAxis),
                deadZoneColor: spread.markArea.itemStyle.color,
                pathSeries: path.series.map(series => ({
                  name: series.name, type: series.type, color: series.lineStyle.color,
                  showSymbol: series.showSymbol,
                })),
                pathDeadZone: path.series[0].markArea.data[0].map(point => point.yAxis),
                pathTooltip: path.tooltip.formatter(path.series.map(series => ({
                  data: series.data[0],
                }))),
              };
            }"""
        )
        assert chart_colors["strongBar"] == {
            "type": "bar",
            "color": "#e66767",
            "plotted": pytest.approx(0.001),
            "raw": pytest.approx(0.001),
        }
        assert chart_colors["weakBar"] == {
            "type": "bar",
            "color": "#24a06b",
            "plotted": pytest.approx(-0.005),
            "raw": pytest.approx(0.005),
        }
        assert chart_colors["spreadLine"] == {
            "type": "line",
            "color": "#4f8fd8",
            "lineType": "solid",
            "showSymbol": False,
        }
        assert chart_colors["axisExtent"][0] == pytest.approx(-chart_colors["axisExtent"][1])
        assert "低位样本 +0.50%" in chart_colors["tooltipText"]
        assert chart_colors["deadZone"] == [-0.0025, 0.0025]
        assert chart_colors["deadZoneColor"] == "rgba(201,150,66,.07)"
        assert chart_colors["pathSeries"] == [
            {"name": "当日强弱差", "type": "line", "color": "#4f8fd8", "showSymbol": True},
            {"name": "三日均值", "type": "line", "color": "#c99642", "showSymbol": False},
        ]
        assert chart_colors["pathDeadZone"] == [-0.0025, 0.0025]
        assert "当日强弱差 +0.10%" in chart_colors["pathTooltip"]
        assert "三日均值 -0.23%" in chart_colors["pathTooltip"]
        distribution = page.locator(".rotation-structure-aside .rotation-section").nth(0)
        path_section = page.locator(".rotation-structure-aside .rotation-section").nth(1)
        main_box = main_chart.bounding_box()
        distribution_box = distribution.bounding_box()
        path_box = path_section.bounding_box()
        assert path_box["x"] > main_box["x"] + main_box["width"]
        assert path_box["y"] > distribution_box["y"]
        assert main_box["height"] == pytest.approx(360, abs=1)
        assert path_chart.bounding_box()["height"] == pytest.approx(136, abs=1)
        assert page.locator(".rotation-path-strip").count() == 0
        chart_state = page.evaluate(
            """() => {
              const option = charts['rotation-style-path-chart'].__qmLastOption;
              return {
                pointCount: option.series[0].data.length,
                seriesCount: option.series.length,
                yAxisType: option.yAxis.type,
                firstSpread: option.series[0].data[0].value,
                firstAverage: option.series[1].data[0].value,
              };
            }"""
        )
        assert chart_state == {
            "pointCount": 15,
            "seriesCount": 2,
            "yAxisType": "value",
            "firstSpread": pytest.approx(0.001),
            "firstAverage": pytest.approx(-0.0023333333333333335),
        }

        page.set_viewport_size({"width": 390, "height": 844})
        page.wait_for_timeout(100)
        mobile_main = main_chart.bounding_box()
        mobile_distribution = distribution.bounding_box()
        mobile_path = path_section.bounding_box()
        assert mobile_distribution["y"] > mobile_main["y"]
        assert mobile_path["y"] > mobile_distribution["y"]
        assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
        assert page_errors == []
        browser.close()


def test_industry_cycle_level_tabs_chart_and_compact_layout(live_server):
    url, _ = live_server

    def industry_item(code, name, level, positive, weak, change, sessions):
        signals = {
            str(window): {
                "positive_change_pp": change / 2,
                "weak_change_pp": -change / 2,
                "rotation_change_pp": change,
                "member_return": 0.012,
                "excess_return": 0.004,
                "advance_ratio": 0.62,
                "amount_activity": 0.08,
            }
            for window in (1, 3, 5, 20)
        }
        return {
            "code": code,
            "name": name,
            "level": level,
            "member_count": 20,
            "eligible_count": 18,
            "coverage": 0.9,
            "positive_ratio": positive,
            "weak_ratio": weak,
            "signals": signals,
            "stage": "repair_spread" if change >= 0 else "retreat_watch",
            "stage_label": "修复扩散" if change >= 0 else "退潮观察",
            "stage_sessions": sessions,
            "score": {
                "window": 5, "score": 68.4, "grade": "B",
                "available_weight": 100, "minimum_weight": 60, "items": [],
            },
        }

    items = [
        industry_item("L1-A", "一级成长", "L1", 58.0, 18.0, 7.0, 6),
        industry_item("L1-B", "一级价值", "L1", 32.0, 44.0, -4.0, 3),
        industry_item("L2-A", "二级软件", "L2", 61.0, 16.0, 9.0, 8),
        industry_item("L2-B", "二级设备", "L2", 38.0, 36.0, -2.0, 4),
        industry_item("L2-C", "未成图二级", "L2", None, None, 1.0, 2),
    ]
    selected_codes = {"L2-A", "L2-B"}
    meta = {
        "as_of": "2026-08-08",
        "algorithm_version": "QM_ROTATION_V7",
        "sources": ["SW2021"],
        "quality": {"status": "complete", "issues": []},
    }

    def industries_handler(route):
        visible = [item for item in items if item["level"] == "L1" or item["code"] in selected_codes]
        route.fulfill(
            json={
                "meta": meta,
                "data": {"items": visible, "summary": {}, "window": 5},
            }
        )

    def preferences_handler(route):
        if route.request.method == "PUT":
            body = route.request.post_data_json
            selected_codes.clear()
            selected_codes.update(body["l2_codes"])
        route.fulfill(
            json={
                "data": {"l2_codes": sorted(selected_codes)},
            }
        )

    taxonomy = {
        "meta": meta,
        "data": {
            "l2": [
                {"code": item["code"], "name": item["name"], "member_count": 20}
                for item in items
                if item["level"] == "L2"
            ],
        },
    }

    with playwright_sync.sync_playwright() as manager:
        browser = manager.chromium.launch()
        page = browser.new_page(
            viewport={"width": 1280, "height": 900},
            reduced_motion="reduce",
        )
        page_errors = []
        console_errors = []
        page.on("pageerror", lambda error: page_errors.append(str(error)))
        page.on(
            "console",
            lambda message: console_errors.append(message.text) if message.type == "error" else None,
        )
        page.route("**/api/v1/rotation/industries**", industries_handler)
        page.route(
            "**/api/v1/rotation/taxonomy/industries",
            lambda route: route.fulfill(json=taxonomy),
        )
        page.route("**/api/v1/rotation/preferences", preferences_handler)
        page.goto(f"{url}/#today/industry")

        l1_tab = page.locator('[data-rotation-industry-level="L1"]')
        l2_tab = page.locator('[data-rotation-industry-level="L2"]')
        playwright_sync.expect(l1_tab).to_have_attribute("aria-selected", "true")
        canvas = page.locator("#rotation-industry-scatter canvas").first
        canvas.wait_for(state="visible")
        assert page.locator("#tab-rotation *").count() <= 3000
        assert _active_chart_count(page) <= 4
        chart_box = page.locator("#rotation-industry-scatter").bounding_box()
        assert chart_box["width"] > 700
        assert chart_box["height"] == pytest.approx(320, abs=1)
        page.wait_for_function(
            """() => charts['rotation-industry-scatter']?.getOption()
              .series.every(series => series.data.length === 2)"""
        )
        axis_bounds = page.evaluate(
            """() => {
              const option = charts['rotation-industry-scatter'].getOption();
              return {
                xMin: option.xAxis[0].min,
                xMax: option.xAxis[0].max,
                yMin: option.yAxis[0].min,
                yMax: option.yAxis[0].max,
              };
            }"""
        )
        assert 0 < axis_bounds["xMin"] < axis_bounds["xMax"] < 100
        assert 0 < axis_bounds["yMin"] < axis_bounds["yMax"] < 100
        chart_panel_box = page.locator(".rotation-industry-chart-panel").bounding_box()
        summary_box = page.locator(".rotation-industry-summary").bounding_box()
        assert summary_box["x"] > chart_panel_box["x"] + chart_panel_box["width"]
        assert summary_box["height"] < chart_panel_box["height"]
        matrix = page.locator(".rotation-industry-matrix")
        playwright_sync.expect(matrix).to_contain_text("一级成长")
        playwright_sync.expect(matrix).not_to_contain_text("二级软件")
        assert matrix.bounding_box()["y"] < 900

        l1_tab.focus()
        page.keyboard.press("ArrowRight")
        playwright_sync.expect(l2_tab).to_have_attribute("aria-selected", "true")
        playwright_sync.expect(matrix).to_contain_text("二级软件")
        playwright_sync.expect(matrix).not_to_contain_text("一级成长")
        page.wait_for_function(
            """() => charts['rotation-industry-scatter']?.getOption()
              .series.every(series => series.data.length === 2)"""
        )

        page.evaluate(
            """() => {
              window.__industryOriginalEchartsInit = window.echarts.init;
              window.echarts.init = () => { throw new Error('synthetic init failure'); };
            }"""
        )
        l1_tab.click()
        playwright_sync.expect(page.locator(".rotation-chart-state")).to_contain_text("周期坐标暂不可用")
        unexpected_page_errors = [error for error in page_errors if error != "synthetic init failure"]
        assert unexpected_page_errors == []
        page_errors.clear()
        page.evaluate(
            """() => {
              window.echarts.init = window.__industryOriginalEchartsInit;
              delete window.__industryOriginalEchartsInit;
            }"""
        )
        l2_tab.click()
        page.locator("#rotation-industry-scatter canvas").first.wait_for(state="visible")

        page.get_by_role("button", name="3 日", exact=True).click()
        playwright_sync.expect(l2_tab).to_have_attribute("aria-selected", "true")
        page.locator("[data-rotation-industry-sort]").select_option("score")
        playwright_sync.expect(l2_tab).to_have_attribute("aria-selected", "true")

        page.get_by_role("button", name="管理关注区", exact=True).click()
        manager_panel = page.locator("#rotation-l2-manager")
        playwright_sync.expect(manager_panel).to_be_visible()
        playwright_sync.expect(page.locator(".rotation-l2-grid")).to_be_visible()
        assert page.locator(".rotation-l2-grid").bounding_box()["height"] <= 280
        page.locator('.rotation-l2-option input[value="L2-A"]').set_checked(False)
        page.locator('.rotation-l2-option input[value="L2-B"]').set_checked(False)
        page.locator('.rotation-l2-option input[value="L2-C"]').set_checked(True)
        page.locator("#rotation-l2-save").click()
        playwright_sync.expect(matrix).to_contain_text("未成图二级")
        playwright_sync.expect(l2_tab).to_have_attribute("aria-selected", "true")
        playwright_sync.expect(page.locator(".rotation-chart-state")).to_contain_text("暂无可绘制坐标")

        page.get_by_role("button", name="管理关注区", exact=True).click()
        page.locator('.rotation-l2-option input[value="L2-C"]').set_checked(False)
        page.locator("#rotation-l2-save").click()
        playwright_sync.expect(matrix).to_contain_text("尚未关注可计算的二级行业")
        playwright_sync.expect(l2_tab).to_have_attribute("aria-selected", "true")

        page.set_viewport_size({"width": 390, "height": 844})
        page.wait_for_timeout(100)
        assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
        assert page_errors == []
        assert console_errors == []
        browser.close()


def test_theme_focus_cards_precede_search_and_complete_catalog(live_server):
    url, _ = live_server

    def theme_item(index: int) -> dict:
        change = float(9 - index)
        signals = {
            str(window): {
                "rotation_change_pp": change,
                "member_return": 0.012 - index * 0.001,
                "excess_return": 0.006 - index * 0.0005,
                "advance_ratio": 0.72 - index * 0.03,
                "amount_activity": 0.09 - index * 0.01,
            }
            for window in (1, 3, 5, 20)
        }
        reasons = [
            {"id": "rotation", "label": "轮动改善"},
            {"id": "excess", "label": "相对收益为正"},
            {"id": "breadth", "label": "上涨宽度过半"},
            {"id": "amount", "label": "量能活跃"},
            {"id": "grade", "label": "周期结构 A/B"},
        ][: max(1, 5 - index // 2)]
        return {
            "code": f"BK{index + 1:04d}",
            "name": ["机器人", "光模块", "创新药", "商业航天", "存储芯片", "液冷服务器"][index],
            "stage": "repair_spread" if index < 4 else "unclear",
            "stage_label": "修复扩散" if index < 4 else "方向未明",
            "stage_sessions": 6 - index,
            "score": {
                "window": 5, "score": 82.4 - index * 4.1,
                "grade": "A" if index < 2 else "B",
                "available_weight": 100, "minimum_weight": 60, "items": [],
            },
            "member_count": 36 + index,
            "eligible_count": 32 + index,
            "coverage": 0.88,
            "signals": signals,
            "primary_industry": {
                "name": "电子" if index != 2 else "医药生物",
                "overlap_count": 12 + index,
                "theme_share": 0.48,
            },
            "representatives": [
                {
                    "name": f"代表样本 {index + 1}",
                    "symbol": f"600{index:03d}",
                    "trend_score": 0.72 - index * 0.03,
                    "return_1d": 0.018 - index * 0.002,
                },
                {
                    "name": f"次代表 {index + 1}",
                    "symbol": f"300{index:03d}",
                    "trend_score": 0.64 - index * 0.02,
                    "return_1d": 0.009 - index * 0.001,
                },
            ],
            "focus": {
                "evidence_count": len(reasons),
                "evidence_total": 5,
                "reasons": reasons,
            },
        }

    items = [theme_item(index) for index in range(6)]
    meta = {
        "as_of": "2026-08-08",
        "algorithm_version": "QM_ROTATION_V7",
        "sources": ["local:bars"],
        "quality": {"status": "complete", "issues": []},
    }

    def themes_handler(route):
        filtered = "query=" in route.request.url
        visible = [] if filtered else items
        route.fulfill(
            json={
                "meta": meta,
                "data": {
                    "items": visible,
                    "focus_items": items[:4],
                    "focus_definition": {
                        "criteria": [
                            {"id": "rotation", "label": "轮动改善"},
                            {"id": "excess", "label": "相对收益为正"},
                            {"id": "breadth", "label": "上涨宽度过半"},
                            {"id": "amount", "label": "量能活跃"},
                            {"id": "grade", "label": "周期结构 A/B"},
                        ],
                        "limit": 4,
                        "window": 5,
                    },
                    "pagination": {
                        "page": 1,
                        "page_size": 50,
                        "total": len(visible),
                        "pages": 1,
                        "has_previous": False,
                        "has_next": False,
                    },
                    "summary": {
                        "group_count": len(items),
                        "movements": {
                            "5": {
                                "improving_count": 5,
                                "retreating_count": 1,
                                "leader": {
                                    "name": "机器人",
                                    "rotation_change_pp": 9.0,
                                },
                            },
                        },
                        "persistence": {
                            "longest": [
                                {"name": "机器人", "sessions": 6, "stage_label": "修复扩散"},
                            ],
                        },
                    },
                },
            }
        )

    with playwright_sync.sync_playwright() as manager:
        browser = manager.chromium.launch()
        page = browser.new_page(
            viewport={"width": 1280, "height": 1000},
            reduced_motion="reduce",
        )
        page_errors = []
        console_errors = []
        page.on("pageerror", lambda error: page_errors.append(str(error)))
        page.on(
            "console",
            lambda message: console_errors.append(message.text) if message.type == "error" else None,
        )
        page.route("**/api/v1/rotation/themes?*", themes_handler)
        page.goto(f"{url}/#today/themes")

        page.get_by_role("heading", name="重点关注题材", exact=True).wait_for()
        cards = page.locator(".rotation-theme-focus-card")
        playwright_sync.expect(cards).to_have_count(4)
        assert page.locator("#tab-rotation *").count() <= 3000
        assert _active_chart_count(page) <= 4
        playwright_sync.expect(cards.nth(0)).to_contain_text("机器人")
        playwright_sync.expect(cards.nth(0)).to_contain_text("5/5 项证据")
        playwright_sync.expect(cards.nth(0)).to_contain_text("代表样本 1")

        lead_box = cards.nth(0).bounding_box()
        compact_box = cards.nth(1).bounding_box()
        search_box = page.locator("[data-rotation-theme-query]").bounding_box()
        table_box = page.locator("#rotation-theme-results table").bounding_box()
        assert lead_box["width"] > compact_box["width"]
        assert lead_box["height"] > compact_box["height"] * 2
        assert search_box["y"] > lead_box["y"] + lead_box["height"]
        assert table_box["y"] > search_box["y"]
        playwright_sync.expect(page.locator("#rotation-theme-results")).to_contain_text("液冷服务器")

        page.locator("[data-rotation-theme-query]").fill("没有匹配")
        page.get_by_text("没有匹配题材", exact=True).wait_for()
        playwright_sync.expect(cards).to_have_count(4)
        playwright_sync.expect(cards.nth(0)).to_contain_text("机器人")

        page.locator("[data-rotation-theme-query]").fill("")
        page.locator("#rotation-theme-results table").wait_for()

        page.set_viewport_size({"width": 390, "height": 844})
        page.wait_for_timeout(100)
        mobile_lead = cards.nth(0).bounding_box()
        mobile_second = cards.nth(1).bounding_box()
        assert mobile_second["y"] > mobile_lead["y"] + mobile_lead["height"]
        assert mobile_second["x"] == pytest.approx(mobile_lead["x"], abs=1)
        assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
        assert page_errors == []
        assert console_errors == []
        browser.close()


def _legacy_market_temperature_change_window_rerenders_cached_evidence(live_server):
    url, _ = live_server
    current_items = [
        {
            "id": identifier,
            "label": label,
            "score": score,
            "weight": weight,
            "note": f"{label}当前证据",
            "available": True,
        }
        for identifier, label, score, weight in [
            ("trend", "趋势分布", 40.0, 40),
            ("breadth", "涨跌宽度", 55.0, 20),
            ("volume", "量能确认", 50.0, 15),
            ("etf_capital", "ETF 资金", 60.0, 15),
            ("sentiment", "情绪代理", 55.0, 10),
        ]
    ]

    def comparison(window: int, change: float, *, partial: bool = False) -> dict:
        compared = []
        for index, item in enumerate(current_items):
            comparable = not partial or index < 3
            previous = item["score"] - change if comparable else None
            compared.append(
                {
                    "id": item["id"],
                    "label": item["label"],
                    "weight": item["weight"],
                    "current_score": item["score"],
                    "previous_score": previous,
                    "change_pp": change if comparable else None,
                    "current_available": True,
                    "previous_available": comparable,
                    "comparable": comparable,
                    "current_note": item["note"],
                    "previous_note": "历史证据" if comparable else "历史证据不足",
                }
            )
        return {
            "window": window,
            "current_as_of": "2026-08-08",
            "reference_as_of": f"2026-07-{30 - window:02d}",
            "temperature": {
                "current": 40.0,
                "previous": 40.0 - change,
                "change_pp": change,
            },
            "evidence": {
                "previous_score": 50.0 - change,
                "previous_available_weight": 100 if not partial else 75,
                "comparable_count": 5 if not partial else 3,
                "total_count": 5,
                "items": compared,
            },
        }

    history = [
        {
            "date": f"2026-07-{day:02d}",
            "temperature": 30.0 + day / 3,
            "ma5": 32.0,
            "ma10": 31.0,
            "ma20": 30.0,
            "eligible": 100,
            "strong_up": 20,
            "up": 20,
            "range": 35,
            "weak": 25,
        }
        for day in range(1, 31)
    ]
    payload = {
        "meta": {
            "as_of": "2026-08-08",
            "algorithm_version": "QM_ROTATION_V7",
            "sources": ["local:bars", "local:news"],
            "quality": {"status": "complete", "issues": []},
        },
        "data": {
            "as_of": "2026-08-08",
            "current": {
                "temperature": 40.0,
                "regime": "expansion",
                "regime_label": "强势扩散区",
                "eligible_count": 100,
                "counts": {"strong_up": 20, "up": 20, "range": 35, "weak": 25},
                "ratios": {"strong_up": 20.0, "up": 20.0, "range": 35.0, "weak": 25.0},
            },
            "history": history,
            "evidence": {"score": 49.75, "available_weight": 100, "items": current_items},
            "change_windows": {
                "default_window": 5,
                "supported_windows": [1, 3, 5, 20],
                "windows": {
                    "1": comparison(1, 1.0),
                    "3": comparison(3, 3.0, partial=True),
                    "5": comparison(5, 5.0),
                    "20": comparison(20, -2.0),
                },
            },
        },
    }
    requests = []

    with playwright_sync.sync_playwright() as manager:
        browser = manager.chromium.launch()
        page = browser.new_page(viewport={"width": 1280, "height": 1000}, reduced_motion="reduce")
        page.add_init_script("localStorage.setItem('quantmaster.rotation.window.v2','20')")
        page.route(
            "**/api/v1/market/temperature",
            lambda route: (requests.append(route.request.url), route.fulfill(json=payload))[-1],
        )
        page.goto(f"{url}/#today/temperature")

        five = page.locator('[data-temperature-window="5"]')
        playwright_sync.expect(five).to_have_attribute("aria-pressed", "true")
        playwright_sync.expect(page.locator(".rotation-kpi").first).to_contain_text("5 日 +5.0 · 升温")
        playwright_sync.expect(page.locator(".rotation-meter-reference")).to_have_count(5)
        etf_row = page.locator(".rotation-evidence-row", has_text="ETF 资金")
        playwright_sync.expect(etf_row).to_contain_text("+5.0")
        page.wait_for_function(
            "window.echarts && echarts.getInstanceByDom(document.getElementById('rotation-evidence-radar'))"
        )
        radar = page.evaluate(
            "echarts.getInstanceByDom(document.getElementById('rotation-evidence-radar')).getOption()"
        )
        assert len(radar["series"]) == 2
        assert radar["series"][1]["lineStyle"]["type"] == "dashed"

        page.locator('[data-temperature-window="20"]').click()
        playwright_sync.expect(page.locator(".rotation-kpi").first).to_contain_text("20 日 -2.0 · 降温")
        assert len(requests) == 1
        assert page.evaluate("localStorage.getItem('quantmaster.rotation.window.v2')") == "20"
        assert page.evaluate("localStorage.getItem('quantmaster.market.temperature-window.v1')") == "20"

        page.locator('[data-temperature-window="1"]').click()
        playwright_sync.expect(page.locator(".rotation-kpi").first).to_contain_text("1 日 +1.0 · 升温")
        playwright_sync.expect(page.locator(".rotation-meter-reference")).to_have_count(5)
        assert len(requests) == 1

        page.locator('[data-temperature-window="3"]').click()
        playwright_sync.expect(page.locator(".rotation-evidence-radar-wrap")).to_contain_text(
            "3 日前仅 3/5 维可比"
        )
        playwright_sync.expect(page.locator(".rotation-meter-reference")).to_have_count(3)
        radar = page.evaluate(
            "echarts.getInstanceByDom(document.getElementById('rotation-evidence-radar')).getOption()"
        )
        assert len(radar["series"]) == 1
        page.set_viewport_size({"width": 390, "height": 844})
        _wait_for_document_fit(page)
        browser.close()


def _legacy_rotation_deep_links_cold_states_and_narrow_layout(live_server):
    url, _ = live_server
    with playwright_sync.sync_playwright() as manager:
        browser = manager.chromium.launch()
        page = browser.new_page(
            viewport={"width": 390, "height": 844},
            reduced_motion="reduce",
        )
        page_errors = []
        console_errors = []
        page.on("pageerror", lambda error: page_errors.append(str(error)))

        def record_console_error(message) -> None:
            if message.type != "error":
                return
            location = message.location or {}
            expected_status_error = (
                "Failed to load resource: the server responded with a status of 503 "
                "(Service Unavailable)"
            )
            if (
                message.text == expected_status_error
                and urlsplit(str(location.get("url") or "")).path
                == "/api/v1/market/overview"
            ):
                return
            console_errors.append(message.text)

        page.on("console", record_console_error)
        page.goto(f"{url}/#today/temperature")

        page.locator("#market-temperature-view").wait_for(state="visible")
        assert page.url.endswith("#today/temperature")
        assert page.locator("#market-temperature-view h2").inner_text() == "市场温度"
        assert page.locator("#market-quotes-view").is_hidden()
        _wait_for_text(page.locator("#market-temperature-content"), "等待")

        page.get_by_role("tab", name="市场风格", exact=True).click()
        page.locator("#market-style-view").wait_for(state="visible")
        assert page.url.endswith("#today/style")

        page.get_by_role("tab", name="轮动总览", exact=True).click()
        page.locator("#rotation-overview-view").wait_for(state="visible")
        playwright_sync.expect(page.get_by_role("heading", name="数据与上游状态")).to_be_visible()
        playwright_sync.expect(page.get_by_role("heading", name="当前数据")).to_be_visible()
        playwright_sync.expect(page.get_by_role("heading", name="上游来源")).to_be_visible()
        assert page.locator("[data-rotation-data-progress]").get_attribute("aria-label")
        assert page.locator(".rotation-source-status details").count() == 2
        assert page.locator(".toast", has_text="上游").count() == 0
        assert page.url.endswith("#today/rotation")
        _wait_for_text(page.locator("#rotation-overview-content"), "等待")
        assert page.locator("#rotation-overview-view #rotation-industry-scatter").count() == 0
        page.goto(f"{url}/#today/rotation")
        page.locator("#rotation-overview-view").wait_for(state="visible")
        assert page.url.endswith("#today/rotation")
        assert "· 0%" not in page.locator('[data-rotation-asof="overview"]').inner_text()
        page.get_by_role("tab", name="行业周期", exact=True).click()
        page.locator("#rotation-industry-view").wait_for(state="visible")
        assert page.url.endswith("#today/industry")
        page.get_by_role("tab", name="细分题材", exact=True).click()
        page.locator("#rotation-themes-view").wait_for(state="visible")
        assert page.url.endswith("#today/themes")
        playwright_sync.expect(page.locator("#rotation-themes-content")).to_contain_text(
            re.compile("等待|计算"),
            timeout=30_000,
        )
        theme_meta = page.locator('[data-rotation-asof="themes"]').inner_text()
        assert any(marker in theme_meta for marker in ("等待刷新", "尚无快照", "正在计算"))
        assert "· 0%" not in theme_meta
        page.get_by_role("tab", name="ETF 研究", exact=True).click()
        page.locator("#rotation-etf-view").wait_for(state="visible")
        assert page.url.endswith("#today/etfs")

        page.reload()
        page.locator("#rotation-etf-view").wait_for(state="visible")
        assert page.url.endswith("#today/etfs")

        page.evaluate("location.hash = '#observe/temperature'")
        page.wait_for_timeout(50)
        assert page.url.endswith("#observe/temperature")
        assert page.locator("#rotation-etf-view").is_visible()
        status_width = page.locator(".rotation-source-status").bounding_box()["width"]
        assert status_width <= 390

        page.evaluate(
            """() => {
              sessionStorage.setItem('quantmaster.workspacePage.v1', JSON.stringify({observe:'news'}));
              sessionStorage.removeItem('quantmaster.workspacePage.v2');
              sessionStorage.removeItem('quantmaster.activeTab');
              history.replaceState(null, '', '/');
            }"""
        )
        page.reload()
        page.wait_for_url(re.compile(r"#today/quotes$"))
        page.locator("#market-quotes-view").wait_for(state="visible")
        assert page.url.endswith("#today/quotes")
        assert page.locator("#tab-news").is_hidden()

        assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
        assert page_errors == []
        assert console_errors == []
        browser.close()


def test_etf_v21_conclusion_first_keyboard_drawer_and_independent_catalog(live_server):
    url, _ = live_server
    sectors = [
        {
            "sector_id": "semi",
            "sector_name": "半导体",
            "category": "行业主题",
            "asset_class": "equity",
            "representative": {"symbol": "512480.SH", "name": "半导体ETF", "normalized_index": "中证半导体"},
            "member_count": 8,
            "index_count": 2,
            "state": "leading",
            "state_label": "领涨共振",
            "trend_strength": 86,
            "activity_score": 82,
            "risk_badges": [{"code": "crowded_high", "label": "高位拥挤风险", "tone": "risk"}],
            "metrics": {
                "return_5d": 0.041,
                "return_20d": 0.128,
                "return_60d": 0.224,
                "position_250d": 91,
                "position_20d": 88,
                "position_60d": 90,
                "drawdown_250d": -0.032,
                "amount_ratio_5v20": 1.42,
            },
            "funds": {
                "status": "confirmed",
                "effective_date": "2026-08-07",
                "coverage": 0.875,
                "share_delta": 120_000_000,
                "share_change_pct": 0.032,
                "estimated_flow": 310_000_000,
                "source": "tushare:etf_share_size",
            },
            "invalidation": "MA20 下穿 MA60，或 20 日收益转负",
        },
        {
            "sector_id": "medicine",
            "sector_name": "医药",
            "category": "行业主题",
            "asset_class": "equity",
            "representative": {"symbol": "512010.SH", "name": "医药ETF", "normalized_index": "中证全指医药"},
            "member_count": 6,
            "index_count": 2,
            "state": "low_turn",
            "state_label": "低位转强",
            "trend_strength": 68,
            "activity_score": 64,
            "risk_badges": [],
            "metrics": {
                "return_5d": 0.026,
                "return_20d": 0.031,
                "return_60d": -0.042,
                "position_250d": 34,
                "position_20d": 67,
                "position_60d": 49,
                "drawdown_250d": -0.28,
                "amount_ratio_5v20": 1.23,
            },
            "funds": {
                "status": "confirmed",
                "effective_date": "2026-08-07",
                "coverage": 0.83,
                "share_delta": 0,
                "share_change_pct": 0,
                "estimated_flow": 0,
                "source": "tushare:etf_share_size",
            },
            "invalidation": "跌回 MA20 下方，或量能比低于 1.10",
        },
        {
            "sector_id": "property",
            "sector_name": "房地产",
            "category": "行业主题",
            "asset_class": "equity",
            "representative": {"symbol": "512200.SH", "name": "房地产ETF", "normalized_index": "中证房地产"},
            "member_count": 3,
            "index_count": 1,
            "state": "weakening",
            "state_label": "走弱",
            "trend_strength": 22,
            "activity_score": 51,
            "risk_badges": [],
            "metrics": {
                "return_5d": -0.032,
                "return_20d": -0.088,
                "return_60d": -0.102,
                "position_250d": 18,
                "position_20d": 12,
                "position_60d": 15,
                "drawdown_250d": -0.39,
                "amount_ratio_5v20": 0.82,
            },
            "funds": {
                "status": "stale",
                "effective_date": "2026-08-06",
                "coverage": 0,
                "share_delta": None,
                "share_change_pct": None,
                "estimated_flow": None,
                "source": "tushare:fund_share",
            },
            "invalidation": "重新站上 MA20 且 5 日收益转正",
        },
    ]
    for sector in sectors:
        sector["display_position"] = sector["metrics"]["position_60d"]
        sector["position_metric"] = "position_60d"
        sector["position_horizon"] = 60
        sector["position_label"] = "60 日阶段位置"
        sector["position_source"] = "stage_research_series"
        sector["candidate_codes"] = []
        sector["candidates"] = {}
        sector["funds"].update(
            {
                "period_kind": "daily",
                "period_sessions": 1,
                "period_label": "当日变化",
                "consecutive": True,
                "coverage_level": "high" if sector["funds"]["coverage"] >= 0.8 else "low",
                "interpretation_note": "连续交易日证据可用于辅助核查主状态",
            }
        )
    sectors[1]["candidate_codes"] = ["stage_low_rebound"]
    sectors[1]["candidates"] = {
        "stage_low_rebound": {
            "label": "阶段低位转强候选",
            "eligible": True,
            "met_conditions": ["60 日阶段位置不高于 40"],
            "unmet_conditions": ["250 日复权位置不高于 40"],
        }
    }
    products = [
        {
            "symbol": sector["representative"]["symbol"],
            "name": sector["representative"]["name"],
            "category": sector["category"],
            "asset_class": "equity",
            "sector_id": sector["sector_id"],
            "sector_name": sector["sector_name"],
            "normalized_index": sector["representative"]["normalized_index"],
            "is_representative": True,
            "sector_state": sector["state"],
            "sector_state_label": sector["state_label"],
            "metrics": {
                **sector["metrics"],
                "avg_amount_20d": 280_000_000,
                "adjustment_status": "raw_short_fallback",
                "display_position": sector["display_position"],
                "position_metric": "position_60d",
            },
            "funds": sector["funds"],
            "metadata": {"total_size": 5_200_000_000, "management_fee": 0.5},
            "coverage": {"daily": True, "adjustment": False, "shares": True},
            "display_position": sector["display_position"],
            "position_label": "60 日阶段位置",
            "candidate_codes": sector["candidate_codes"],
        }
        for sector in sectors
    ]
    overview = {
        "meta": {
            "snapshot_id": "etf_v3_demo",
            "as_of": "2026-08-07",
            "staleness": {"stale": False},
            "quality": {"status": "complete"},
        },
        "data": {
            "freshness": {
                key: {"date": "2026-08-07", "status": "ready", "coverage": coverage}
                for key, coverage in {
                    "market": 1,
                    "shares": 0.87,
                    "adjustment": 0,
                    "metadata": 1,
                }.items()
            },
            "capabilities": {},
            "summaries": [
                {
                    "kind": "strongest",
                    "title": "最强板块",
                    "sector_id": "semi",
                    "sector_name": "半导体",
                    "state": "leading",
                    "text": "半导体 · 领涨共振 · 趋势 86",
                    "evaluation_status": "confirmed",
                },
                {
                    "kind": "low_turn",
                    "title": "低位转强",
                    "sector_id": "medicine",
                    "sector_name": "医药",
                    "state": "low_turn",
                    "text": "医药 · 低位转强 · 趋势 68",
                    "evaluation_status": "candidate",
                },
                {
                    "kind": "risk",
                    "title": "主要风险",
                    "sector_id": "semi",
                    "sector_name": "半导体",
                    "state": "leading",
                    "text": "半导体 · 高位拥挤风险",
                    "evaluation_status": "confirmed",
                },
            ],
            "sectors": sectors,
            "queues": {
                "leading": ["semi"],
                "low_turn": ["medicine"],
                "improving": [],
                "weakening": ["property"],
                "watch": [],
                "risk": ["semi"],
            },
            "candidate_queues": {
                "momentum_hot": [],
                "stage_low_rebound": ["medicine"],
                "stage_high_activity": [],
            },
            "map": {
                "position_metric": "position_60d",
                "horizon": 60,
                "label": "60 日阶段位置",
                "coverage": 1,
                "sector_ids": ["semi", "medicine", "property"],
            },
        },
    }
    history = [
        {"date": f"2026-08-{day:02d}", "price": 96 + day, "amount": 100_000_000 + day * 5_000_000}
        for day in range(1, 8)
    ]
    fund_history = [
        {
            "date": f"2026-08-{day:02d}",
            "share_delta": day * 10_000_000,
            "estimated_flow": day * 20_000_000,
            "confirmed_members": 6,
        }
        for day in range(1, 8)
    ]

    intraday_calls = {"count": 0}

    def route_api(route):
        request_url = route.request.url
        path = request_url.split("?", 1)[0]
        if "/rotation/etfs/overview" in request_url:
            route.fulfill(json=overview)
        elif "/rotation/etfs/sectors/" in request_url:
            selected = next(item for item in sectors if item["sector_id"] in request_url)
            route.fulfill(
                json={
                    "meta": overview["meta"],
                    "data": {
                        **selected,
                        "history": history,
                        "members": products,
                        "member_pagination": {
                            "page": 1,
                            "page_size": 25,
                            "total": len(products),
                            "pages": 1,
                            "has_previous": False,
                            "has_next": False,
                        },
                        "index_groups": [
                            {
                                "index_key": selected["representative"]["normalized_index"],
                                "normalized_index": selected["representative"]["normalized_index"],
                                "member_count": len(products),
                            }
                        ],
                        "selected_index_key": "",
                        "funds": {
                            **selected["funds"],
                            "history": fund_history,
                            "provenance_note": "份额只解释一级市场申赎；二级市场成交不会自动改变总份额。",
                        },
                        "explanation": {"conclusion": selected["state_label"]},
                    },
                }
            )
        elif path.endswith("/rotation/etfs/512480.SH/intraday"):
            intraday_calls["count"] += 1
            route.fulfill(
                json={
                    "meta": {"snapshot_id": "etf_v3_demo", "as_of": "2026-08-07"},
                    "data": {
                        "symbol": "512480.SH",
                        "date": "2026-08-07",
                        "status": "ready",
                        "cache_hit": False,
                        "series": [
                            {"time": "2026-08-07T09:30", "close": 1.0},
                            {"time": "2026-08-07T09:31", "close": 1.01},
                        ],
                    },
                }
            )
        elif path.endswith("/rotation/etfs/512480.SH"):
            selected = products[0]
            route.fulfill(
                json={
                    "meta": overview["meta"],
                    "data": {
                        **selected,
                        "metrics": {**sectors[0]["metrics"], "avg_amount_20d": 280_000_000},
                        "sector_state": "leading",
                        "sector_state_label": "领涨共振",
                        "trend_strength": 86,
                        "activity_score": 82,
                        "risk_badges": sectors[0]["risk_badges"],
                        "candidate_codes": [],
                        "candidates": {},
                        "invalidation": sectors[0]["invalidation"],
                        "history": history,
                        "peer_products": [],
                    },
                }
            )
        elif "/rotation/etfs/snapshots" in request_url:
            route.fulfill(json={"items": [{"snapshot_id": "etf_v3_demo", "as_of_date": "2026-08-07"}]})
        elif "/rotation/etfs" in request_url:
            route.fulfill(
                json={
                    "meta": overview["meta"],
                    "data": {
                        "items": products,
                        "categories": ["行业主题"],
                        "pagination": {
                            "page": 1,
                            "page_size": 50,
                            "total": 3,
                            "pages": 1,
                            "has_previous": False,
                            "has_next": False,
                        },
                    },
                }
            )
        else:
            route.continue_()

    with playwright_sync.sync_playwright() as manager:
        browser = manager.chromium.launch()
        page = browser.new_page(viewport={"width": 1280, "height": 720})
        page.route("**/api/v1/rotation/etfs**", route_api)
        page_errors = []
        page.on("pageerror", lambda error: page_errors.append(str(error)))
        page.goto(f"{url}/#today/etfs")
        page.locator("#rotation-etf-view").wait_for(state="visible")
        playwright_sync.expect(page.locator(".etf-summary")).to_have_count(3)
        assert page.locator("#tab-rotation *").count() <= 3000
        assert _active_chart_count(page) <= 4
        assert page.locator(".etf-summary:not([disabled])").evaluate_all(
            "nodes => nodes.every(node => node.getAttribute('aria-haspopup') === 'dialog')"
        )
        playwright_sync.expect(page.locator("#rotation-etf-map")).to_be_visible()
        playwright_sync.expect(page.locator(".etf-queues")).to_contain_text("领涨")
        assert page.locator("#rotation-etf-map").bounding_box()["height"] == 320
        queue = page.locator(".etf-queues")
        queue_groups = queue.locator(".etf-queue-group")
        playwright_sync.expect(queue_groups).to_have_count(6)
        assert queue.locator(".etf-queue-group[open]").count() == 1
        assert queue.bounding_box()["height"] <= 436
        queue_groups.nth(1).locator("summary").click()
        playwright_sync.expect(queue_groups.nth(1)).to_have_attribute("open", "")
        assert queue.locator(".etf-queue-group[open]").count() == 1
        assert queue_groups.nth(1).locator(":scope > div").evaluate(
            "node => getComputedStyle(node).overflowY"
        ) == "auto"
        assert page.locator("#rotation-etf-product-results tbody tr").count() <= 50
        product_results = page.locator("#rotation-etf-product-results")
        playwright_sync.expect(product_results).to_contain_text("+1.20 亿份（+3.20%）· 估算净申购3.10 亿元")
        playwright_sync.expect(product_results).to_contain_text("0 份（0.00%）· 已确认当日无净申赎")
        assert "估算净申购+" not in product_results.inner_text()
        for selector in (".etf-freshness", ".etf-summary-grid"):
            box = page.locator(selector).bounding_box()
            assert box and box["y"] + box["height"] <= 720, (selector, box)

        drawer = page.locator("#rotation-etf-detail")
        summary_titles = ("半导体", "医药", "半导体")
        for index, title in enumerate(summary_titles):
            trigger = page.locator(".etf-summary:not([disabled])").nth(index)
            trigger.click()
            playwright_sync.expect(drawer).to_be_visible()
            playwright_sync.expect(drawer.locator("h3")).to_have_text(title)
            playwright_sync.expect(drawer.locator('[data-etf-drawer-panel="conclusion"]')).to_be_visible()
            playwright_sync.expect(drawer.locator(".etf-drawer-close")).to_be_visible()
            drawer_box = drawer.bounding_box()
            assert drawer_box and drawer_box["x"] >= 0 and drawer_box["y"] >= 0
            assert drawer_box["x"] + drawer_box["width"] <= 1280
            assert drawer_box["y"] + drawer_box["height"] <= 720
            assert drawer.locator("[data-etf-drawer-body]").evaluate(
                "node => getComputedStyle(node).overflowY"
            ) == "auto"
            drawer.locator(".etf-drawer-close").click()
            playwright_sync.expect(drawer).to_be_hidden()
            assert trigger.evaluate("node => document.activeElement === node")

        queue_groups.first.locator("summary").click()
        queue_trigger = queue_groups.first.locator("[data-etf-sector]").first
        queue_trigger.click()
        playwright_sync.expect(drawer.locator("h3")).to_have_text("半导体")
        page.keyboard.press("Escape")
        playwright_sync.expect(drawer).to_be_hidden()
        assert queue_trigger.evaluate("node => document.activeElement === node")

        category_filter = page.locator("[data-rotation-etf-category]")
        category_filter.select_option("行业主题")
        asset_tabs = page.locator(".etf-asset-tabs [role='tab']")
        asset_tabs.first.focus()
        with page.expect_response(
            lambda response: "/rotation/etfs/overview?" in response.url
            and "asset=overseas_equity" in response.url
        ) as asset_response:
            page.keyboard.press("ArrowRight")
        asset_response.value.finished()
        playwright_sync.expect(asset_tabs.nth(1)).to_have_attribute("aria-selected", "true")
        playwright_sync.expect(asset_tabs.nth(1)).to_be_focused()
        playwright_sync.expect(page.locator("[data-rotation-etf-category]")).to_have_value("")
        asset_tabs.first.click()

        map_select = page.locator("[data-etf-map-select]")
        map_select.focus()
        map_select.select_option("semi")
        drawer.wait_for(state="visible")
        conclusion_tab = drawer.locator('[data-etf-drawer-tab="conclusion"]')
        playwright_sync.expect(conclusion_tab).to_be_visible()
        conclusion_tab.focus()
        page.keyboard.press("ArrowRight")
        playwright_sync.expect(drawer.locator('[data-etf-drawer-panel="trend"]')).to_be_visible()
        assert intraday_calls["count"] == 0
        page.keyboard.press("ArrowRight")
        playwright_sync.expect(drawer.locator('[data-etf-drawer-panel="funds"]')).to_be_visible()
        page.keyboard.press("ArrowRight")
        product_panel = drawer.locator('[data-etf-drawer-panel="products"]')
        playwright_sync.expect(product_panel).to_be_visible()
        playwright_sync.expect(product_panel).to_contain_text("52.00 亿元")
        playwright_sync.expect(product_panel).to_contain_text("第 1/1 页")
        page.keyboard.press("Escape")
        playwright_sync.expect(drawer).to_be_hidden()
        assert page.evaluate("document.activeElement === document.querySelector('[data-etf-map-select]')")

        selected_product = page.locator("#rotation-etf-product-results [data-etf-detail='512480.SH']")
        selected_product.click()
        drawer.wait_for(state="visible")
        playwright_sync.expect(drawer.locator("h3")).to_have_text("半导体ETF")
        assert intraday_calls["count"] == 0
        drawer.locator('[data-etf-drawer-tab="trend"]').click()
        for _ in range(20):
            if intraday_calls["count"] == 1:
                break
            page.wait_for_timeout(25)
        assert intraday_calls["count"] == 1
        playwright_sync.expect(drawer.locator("[data-etf-intraday-status]")).to_contain_text("按需读取")
        page.keyboard.press("Escape")
        playwright_sync.expect(drawer).to_be_hidden()

        page.set_viewport_size({"width": 1024, "height": 768})
        page.evaluate("document.documentElement.style.zoom = '2'")
        playwright_sync.expect(page.locator(".rotation-etf-research-table")).to_have_count(2)
        page.wait_for_timeout(150)
        _wait_for_document_fit(page)
        assert page.locator("#rotation-etf-view .rotation-table-wrap").evaluate_all(
            "nodes => nodes.every(node => node.getBoundingClientRect().right <= "
            "document.querySelector('#rotation-etf-view').getBoundingClientRect().right + 1)"
        )
        page.evaluate("document.documentElement.style.zoom = ''")

        page.set_viewport_size({"width": 390, "height": 844})
        _wait_for_document_fit(page)
        mobile_trigger = page.locator(".etf-summary:not([disabled])").first
        mobile_trigger.click()
        playwright_sync.expect(drawer).to_be_visible()
        mobile_box = drawer.bounding_box()
        assert mobile_box and mobile_box["x"] >= 0 and mobile_box["y"] >= 0
        assert mobile_box["x"] + mobile_box["width"] <= 390
        assert mobile_box["y"] + mobile_box["height"] <= 844
        drawer.locator(".etf-drawer-close").click()
        playwright_sync.expect(drawer).to_be_hidden()
        assert page_errors == []
        browser.close()


def test_etf_v21_auto_research_runs_once_and_returns_to_latest(live_server):
    url, _ = live_server
    calls = {"scan": 0, "published": False}
    overview = {
        "meta": {
            "snapshot_id": "etf_old_snapshot",
            "quality": {"status": "complete", "issues": []},
            "refresh": {
                "recommended": True,
                "input_id": "evidence_fingerprint_20260807",
                "input_as_of": "2026-08-07",
                "reason": "本地证据已变化（份额），进入页面时仅补算一次",
            },
        },
        "data": {
            "freshness": {}, "capabilities": {}, "summaries": [], "sectors": [],
            "queues": {}, "candidate_queues": {},
            "map": {
                "position_metric": "position_60d",
                "horizon": 60,
                "label": "60 日阶段位置",
                "coverage": 0,
                "sector_ids": [],
            },
        },
    }

    def route_api(route):
        request = route.request
        path = request.url.split("?", 1)[0]
        if path.endswith("/rotation/etfs/overview"):
            payload = json.loads(json.dumps(overview))
            if calls["published"]:
                payload["meta"]["snapshot_id"] = "etf_new_snapshot"
                payload["meta"]["refresh"] = {
                    "recommended": False,
                    "input_id": "evidence_fingerprint_20260807",
                    "input_as_of": "2026-08-07",
                    "reason": "研究快照已使用最新证据",
                }
            route.fulfill(json=payload)
        elif path.endswith("/rotation/etfs/snapshots"):
            route.fulfill(json={"items": [
                {"snapshot_id": "etf_new_snapshot", "as_of_date": "2026-08-07"},
                {"snapshot_id": "etf_old_snapshot", "as_of_date": "2026-08-06"},
            ]})
        elif path.endswith("/rotation/etfs/scan") and request.method == "POST":
            calls["scan"] += 1
            calls["published"] = True
            route.fulfill(
                status=202,
                json={
                    "id": "etf-auto-job",
                    "status": "completed",
                    "progress": 100,
                    "phase": "已完成",
                    "message": "ETF 研究快照已发布",
                    "can_cancel": False,
                    "created": True,
                },
            )
        elif path.endswith("/rotation/etfs"):
            route.fulfill(
                json={
                    "meta": overview["meta"],
                    "data": {
                        "items": [],
                        "categories": [],
                        "pagination": {"page": 1, "page_size": 50, "total": 0, "pages": 1},
                    },
                },
            )
        else:
            route.continue_()

    with playwright_sync.sync_playwright() as manager:
        browser = manager.chromium.launch()
        page = browser.new_page(viewport={"width": 1280, "height": 720})
        page.route("**/api/v1/rotation/etfs**", route_api)
        page.goto(f"{url}/#today/etfs")
        for _ in range(40):
            if calls["scan"] == 1:
                break
            page.wait_for_timeout(50)
        assert calls["scan"] == 1
        playwright_sync.expect(page.locator("#rotation-etf-job")).to_be_hidden()
        playwright_sync.expect(page.locator("#rotation-etf-history")).to_have_value("")
        playwright_sync.expect(page.locator("#rotation-etf-json")).to_have_attribute(
            "href", re.compile("etf_new_snapshot")
        )

        page.reload()
        page.wait_for_timeout(500)
        assert calls["scan"] == 1
        browser.close()


def test_stock_analysis_progressive_restore_and_reduced_motion(live_server):
    url, _ = live_server
    keys = [
        ("fundamental", "①", "基本面"),
        ("technical", "②", "技术面"),
        ("news", "③", "消息面"),
        ("capital", "④", "资金面"),
        ("sentiment", "⑤", "市场心理面"),
        ("macro", "⑥", "宏观/政策面"),
    ]
    dimensions = [
        {
            "key": key,
            "number": number,
            "title": title,
            "score": 61 + index,
            "stance": "谨慎偏强",
            "status": "complete",
            "summary": f"{title}证据已完成复核。",
            "metrics": [{"label": "样本指标", "value": index, "display": str(index), "note": ""}],
            "signals": ["证据支持当前方向。"],
            "risks": ["仍需等待后续数据。"],
            "as_of": "2026-07-30",
            "generation": "llm_assisted",
            "degraded_reason": "",
            "evidence_ids": [f"ev_{index:020d}"],
            "evidence": [
                {
                    "id": f"ev_{index:020d}",
                    "title": f"{title}来源",
                    "value": {"sample": index},
                    "excerpt": "可核查摘要",
                    "published_at": "2026-07-30",
                    "data_as_of": "2026-07-30",
                    "source": {"name": "官方来源", "level": 1, "url": f"https://example.com/{key}"},
                }
            ],
        }
        for index, (key, number, title) in enumerate(keys)
    ]
    dimensions[0]["summary"] = {
        "text": "基本面结构化信封已转换为正文。",
        "evidence_ids": [dimensions[0]["evidence_ids"][0]],
    }
    report = {
        "schema_version": "2.0",
        "instrument": {
            "symbol": "600519.SH",
            "name": "贵州茅台",
            "market_label": "中国内地",
        },
        "quote": {"current": 1500, "change_pct": 1.25},
        "data_as_of": "2026-07-30",
        "overall": {
            "score": 65.5,
            "stance": "谨慎偏强",
            "coverage": 100,
            "confidence": 85,
            "thesis": "六维证据总体偏强，但仍需等待新披露。",
            "summary": "终审已检查证据时点与冲突。",
            "risks": ["市场波动可能放大。"],
        },
        "dimensions": dimensions,
        "scenarios": [
            {
                "title": "基准情景",
                "priority": "当前主场景",
                "condition": "价格维持区间。",
                "response": "等待新证据。",
            }
        ],
        "warnings": [],
        "research": {
            "mode": "deep",
            "elapsed_seconds": 128,
            "evidence_count": 6,
            "sources": [{"id": "src_1"}],
        },
        "disclaimer": "仅作量化研究与记录，不构成投资建议。",
    }
    submitted = []
    event_calls = {"count": 0}

    def route_api(route):
        request = route.request
        path = request.url.split("?", 1)[0]
        if path.endswith("/api/v1/market/stock-analyses") and request.method == "POST":
            submitted.append(request.post_data_json)
            route.fulfill(
                status=202,
                json={
                    "analysis_id": "analysis-stock",
                    "job_id": "job-stock",
                    "status": "queued",
                },
            )
        elif path.endswith("/api/v1/market/stock-analyses/analysis-stock"):
            route.fulfill(json={"analysis_id": "analysis-stock", "status": "completed", "report": report})
        elif path.endswith("/api/v1/jobs/job-stock/events"):
            event_calls["count"] += 1
            items = (
                []
                if event_calls["count"] > 1
                else [
                    {
                        "seq": index + 1,
                        "type": "dimension_completed",
                        "payload": {
                            "dimension": item["key"],
                            "result": item,
                            "completed": index + 1,
                        },
                    }
                    for index, item in enumerate(dimensions)
                ]
            )
            route.fulfill(json={"items": items})
        elif path.endswith("/api/v1/jobs/job-stock"):
            route.fulfill(
                json={
                    "id": "job-stock",
                    "status": "completed",
                    "progress": 100,
                    "phase": "分析完成",
                    "estimated_remaining_seconds": 0,
                }
            )
        else:
            route.fallback()

    with playwright_sync.sync_playwright() as manager:
        browser = manager.chromium.launch()
        context = browser.new_context(viewport={"width": 1280, "height": 900}, reduced_motion="reduce")
        page = context.new_page()
        page.route("**/api/v1/**", route_api)
        page.goto(url)
        page.get_by_role("button", name="今日", exact=True).click()
        page.get_by_role("tab", name="个股分析", exact=True).click()
        page.locator("#stock-analysis-query").fill("600519.SH")
        assert page.locator('input[name="mode"][value="deep"]').is_checked()
        page.locator("#stock-analysis-form button.primary").click()
        page.get_by_text("六维证据总体偏强，但仍需等待新披露。").wait_for()
        page.get_by_text("基本面结构化信封已转换为正文。").wait_for()

        assert submitted == [{"query": "600519.SH", "mode": "deep"}]
        assert page.locator(".sa-dimension").count() == 6
        assert "evidence_ids" not in page.locator("#stock-analysis-report").inner_text()
        assert page.locator('.sa-citations a[href="https://example.com/fundamental"]').count() == 1
        assert page.locator("#stock-analysis-elapsed").inner_text()
        assert page.evaluate("getComputedStyle(document.querySelector('.sa-report')).animationName") == "none"

        page.reload()
        page.locator("#stock-analysis-query").wait_for(state="visible")
        page.get_by_text("六维证据总体偏强，但仍需等待新披露。").wait_for()
        restored = page.evaluate("JSON.parse(localStorage.getItem('qm.stock-analysis.active.v2'))")
        assert restored["jobId"] == "job-stock"
        browser.close()


def test_stock_analysis_remount_resumes_nonterminal_poll_and_clock(live_server):
    url, _ = live_server
    event_calls = {"count": 0}

    def route_api(route):
        path = route.request.url.split("?", 1)[0]
        if path.endswith("/api/v1/market/stock-analyses/analysis-active"):
            route.fulfill(json={"analysis_id": "analysis-active", "status": "running"})
        elif path.endswith("/api/v1/jobs/job-active/events"):
            event_calls["count"] += 1
            route.fulfill(json={"items": []})
        elif path.endswith("/api/v1/jobs/job-active"):
            route.fulfill(json={
                "id": "job-active", "status": "running", "progress": 20,
                "phase": "证据采集中", "estimated_remaining_seconds": 30,
            })
        else:
            route.fallback()

    with playwright_sync.sync_playwright() as manager:
        browser = manager.chromium.launch()
        page = browser.new_page(viewport={"width": 1280, "height": 900})
        page.add_init_script(
            """localStorage.setItem('qm.stock-analysis.active.v2', JSON.stringify({
              analysisId:'analysis-active', jobId:'job-active', query:'600519.SH', mode:'deep',
              status:'running', phase:'证据采集中', startedAt:Date.now() - 3000, eta:30,
            }));"""
        )
        page.route("**/api/v1/**", route_api)
        page.goto(f"{url}/#today/stock-analysis")
        page.get_by_text("证据采集中", exact=True).wait_for()
        page.wait_for_timeout(1_100)
        assert event_calls["count"] >= 1

        page.get_by_role("button", name="账户", exact=True).click()
        page.wait_for_url(re.compile(r"#account/paper$"))
        paused_calls = event_calls["count"]
        page.wait_for_timeout(1_100)
        assert event_calls["count"] == paused_calls

        page.get_by_role("button", name="今日", exact=True).click()
        page.wait_for_url(re.compile(r"#today/stock-analysis$"))
        elapsed = page.locator("#stock-analysis-elapsed").inner_text()
        page.wait_for_timeout(1_100)
        assert event_calls["count"] > paused_calls
        assert page.locator("#stock-analysis-elapsed").inner_text() != elapsed
        browser.close()


def test_stock_analysis_ignores_delayed_poll_from_previous_mount(live_server):
    url, _ = live_server
    with playwright_sync.sync_playwright() as manager:
        browser = manager.chromium.launch()
        page = browser.new_page(viewport={"width": 1280, "height": 900})
        page.goto(f"{url}/#today/stock-analysis")
        page.locator("#stock-analysis-query").wait_for(state="visible")
        page.evaluate(
            """() => {
              const nativeApi = window.QuantMasterAPI;
              let jobCalls = 0;
              window.__stockOldJobPending = false;
              window.__resolveStockOldJob = null;
              window.QuantMasterAPI = (input, options) => {
                const path = new URL(input, location.href).pathname;
                if (path === '/api/v1/market/stock-analyses' && options?.method === 'POST') {
                  return Promise.resolve({
                    analysis_id:'analysis-race', job_id:'job-race', status:'running',
                  });
                }
                if (path === '/api/v1/jobs/job-race/events') return Promise.resolve({items:[]});
                if (path === '/api/v1/jobs/job-race') {
                  jobCalls += 1;
                  if (jobCalls === 1) {
                    window.__stockOldJobPending = true;
                    return new Promise(resolve => {
                      window.__resolveStockOldJob = () => resolve({
                        id:'job-race', status:'running', progress:20,
                        phase:'旧响应不应覆盖', estimated_remaining_seconds:30,
                      });
                    });
                  }
                  return Promise.resolve({
                    id:'job-race', status:'completed', progress:100,
                    phase:'新任务已完成', estimated_remaining_seconds:0,
                  });
                }
                if (path === '/api/v1/market/stock-analyses/analysis-race') {
                  return Promise.resolve({analysis_id:'analysis-race', status:'running'});
                }
                return nativeApi(input, options);
              };
            }"""
        )
        page.locator("#stock-analysis-query").fill("600519.SH")
        page.locator("#stock-analysis-form button.primary").click()
        page.wait_for_function("() => window.__stockOldJobPending === true")

        page.get_by_role("button", name="账户", exact=True).click()
        page.wait_for_url(re.compile(r"#account/paper$"))
        page.get_by_role("button", name="今日", exact=True).click()
        page.wait_for_url(re.compile(r"#today/stock-analysis$"))
        page.get_by_text("新任务已完成", exact=True).wait_for()

        page.evaluate("window.__resolveStockOldJob()")
        page.wait_for_timeout(200)
        assert page.locator("#stock-analysis-current-phase").inner_text() == "新任务已完成"
        restored = page.evaluate("JSON.parse(localStorage.getItem('qm.stock-analysis.active.v2'))")
        assert restored["status"] == "completed"
        browser.close()


def test_stock_analysis_slow_submit_persists_run_without_polling_until_remount(live_server):
    url, _ = live_server
    with playwright_sync.sync_playwright() as manager:
        browser = manager.chromium.launch()
        page = browser.new_page(viewport={"width": 1280, "height": 900})
        page.goto(f"{url}/#today/stock-analysis")
        page.locator("#stock-analysis-query").wait_for(state="visible")
        page.evaluate(
            """() => {
              const nativeApi = window.QuantMasterAPI;
              window.__submitPending = false;
              window.__hiddenPolls = 0;
              window.__resolveSubmit = null;
              window.QuantMasterAPI = (input, options = {}) => {
                const path = new URL(input, location.href).pathname;
                if (path === '/api/v1/market/stock-analyses' && options.method === 'POST') {
                  window.__submitPending = true;
                  return new Promise(resolve => {
                    window.__resolveSubmit = () => resolve({
                      analysis_id:'analysis-hidden', job_id:'job-hidden', status:'queued',
                    });
                  });
                }
                if (path === '/api/v1/jobs/job-hidden/events') {
                  window.__hiddenPolls += 1;
                  return Promise.resolve({items:[]});
                }
                if (path === '/api/v1/jobs/job-hidden') {
                  window.__hiddenPolls += 1;
                  return Promise.resolve({
                    id:'job-hidden', status:'completed', progress:100,
                    phase:'离开期间提交的任务已恢复', estimated_remaining_seconds:0,
                  });
                }
                if (path === '/api/v1/market/stock-analyses/analysis-hidden') {
                  return Promise.resolve({analysis_id:'analysis-hidden', status:'completed'});
                }
                return nativeApi(input, options);
              };
            }"""
        )
        page.locator("#stock-analysis-query").fill("600519.SH")
        page.locator("#stock-analysis-form button.primary").click()
        page.wait_for_function("() => window.__submitPending === true")

        page.get_by_role("button", name="账户", exact=True).click()
        page.wait_for_url(re.compile(r"#account/paper$"))
        page.evaluate("window.__resolveSubmit()")
        page.wait_for_timeout(200)

        assert page.evaluate("window.__hiddenPolls") == 0
        stored = page.evaluate("JSON.parse(localStorage.getItem('qm.stock-analysis.active.v2'))")
        assert stored["jobId"] == "job-hidden"

        page.get_by_role("button", name="今日", exact=True).click()
        page.wait_for_url(re.compile(r"#today/stock-analysis$"))
        page.wait_for_function("() => window.__hiddenPolls >= 2")
        page.get_by_text("离开期间提交的任务已恢复", exact=True).wait_for()
        browser.close()


def test_stock_analysis_late_submit_response_cannot_replace_newer_run(live_server):
    url, _ = live_server
    with playwright_sync.sync_playwright() as manager:
        browser = manager.chromium.launch()
        page = browser.new_page(viewport={"width": 1280, "height": 900})
        page.goto(f"{url}/#today/stock-analysis")
        page.locator("#stock-analysis-query").wait_for(state="visible")
        page.evaluate(
            """() => {
              const nativeApi = window.QuantMasterAPI;
              window.__oldSubmitPending = false;
              window.__resolveOldSubmit = null;
              window.QuantMasterAPI = (input, options = {}) => {
                const path = new URL(input, location.href).pathname;
                if (path === '/api/v1/market/stock-analyses' && options.method === 'POST') {
                  const query = JSON.parse(options.body).query;
                  if (query === '600519.SH') {
                    window.__oldSubmitPending = true;
                    return new Promise(resolve => {
                      window.__resolveOldSubmit = () => resolve({
                        analysis_id:'analysis-old-submit', job_id:'job-old-submit', status:'running',
                      });
                    });
                  }
                  return Promise.resolve({
                    analysis_id:'analysis-new-submit', job_id:'job-new-submit', status:'running',
                  });
                }
                if (path.endsWith('/events')) return Promise.resolve({items:[]});
                if (path === '/api/v1/jobs/job-new-submit') {
                  return Promise.resolve({
                    id:'job-new-submit', status:'running', progress:30, phase:'新提交正在运行',
                  });
                }
                return nativeApi(input, options);
              };
            }"""
        )
        query = page.locator("#stock-analysis-query")
        query.fill("600519.SH")
        page.locator("#stock-analysis-form button.primary").click()
        page.wait_for_function("() => window.__oldSubmitPending === true")

        query.fill("000001.SZ")
        page.locator("#stock-analysis-form").evaluate("form => form.requestSubmit()")
        page.get_by_text("新提交正在运行", exact=True).wait_for()
        page.evaluate("window.__resolveOldSubmit()")
        page.wait_for_timeout(100)

        active = page.evaluate("JSON.parse(localStorage.getItem('qm.stock-analysis.active.v2'))")
        assert active["jobId"] == "job-new-submit"
        assert page.locator("#stock-analysis-current-phase").inner_text() == "新提交正在运行"
        browser.close()


def test_stock_analysis_slow_cancel_does_not_mutate_newer_run(live_server):
    url, _ = live_server
    with playwright_sync.sync_playwright() as manager:
        browser = manager.chromium.launch()
        page = browser.new_page(viewport={"width": 1280, "height": 900})
        page.goto(f"{url}/#today/stock-analysis")
        page.locator("#stock-analysis-query").wait_for(state="visible")
        page.evaluate(
            """() => {
              const nativeApi = window.QuantMasterAPI;
              window.__cancelPending = false;
              window.__resolveCancel = null;
              window.QuantMasterAPI = (input, options = {}) => {
                const path = new URL(input, location.href).pathname;
                if (path === '/api/v1/market/stock-analyses' && options.method === 'POST') {
                  const query = JSON.parse(options.body).query;
                  const suffix = query === '600519.SH' ? 'old' : 'new';
                  return Promise.resolve({
                    analysis_id:`analysis-${suffix}`, job_id:`job-${suffix}`, status:'running',
                  });
                }
                if (path.endsWith('/events')) return Promise.resolve({items:[]});
                if (path === '/api/v1/jobs/job-old/cancel') {
                  window.__cancelPending = true;
                  return new Promise(resolve => {
                    window.__resolveCancel = () => resolve({status:'cancelling'});
                  });
                }
                if (path === '/api/v1/jobs/job-old') {
                  return Promise.resolve({status:'running', progress:20, phase:'旧任务运行中'});
                }
                if (path === '/api/v1/jobs/job-new') {
                  return Promise.resolve({status:'running', progress:30, phase:'新任务运行中'});
                }
                return nativeApi(input, options);
              };
            }"""
        )
        query = page.locator("#stock-analysis-query")
        query.fill("600519.SH")
        page.locator("#stock-analysis-form button.primary").click()
        page.get_by_text("旧任务运行中", exact=True).wait_for()
        page.locator("#stock-analysis-cancel").click()
        page.wait_for_function("() => window.__cancelPending === true")

        query.fill("000001.SZ")
        page.locator("#stock-analysis-form button.primary").click()
        page.get_by_text("新任务运行中", exact=True).wait_for()
        page.evaluate("window.__resolveCancel()")
        page.wait_for_timeout(100)

        active = page.evaluate("JSON.parse(localStorage.getItem('qm.stock-analysis.active.v2'))")
        assert active["jobId"] == "job-new"
        assert active["status"] == "running"
        assert page.locator("#stock-analysis-current-phase").inner_text() == "新任务运行中"
        browser.close()


def test_stock_analysis_cancel_uses_endpoint_terminal_status(live_server):
    url, _ = live_server
    with playwright_sync.sync_playwright() as manager:
        browser = manager.chromium.launch()
        page = browser.new_page(viewport={"width": 1280, "height": 900})
        page.goto(f"{url}/#today/stock-analysis")
        page.locator("#stock-analysis-query").wait_for(state="visible")
        page.evaluate(
            """() => {
              const nativeApi = window.QuantMasterAPI;
              window.__cancelJobPolls = 0;
              window.QuantMasterAPI = (input, options = {}) => {
                const path = new URL(input, location.href).pathname;
                if (path === '/api/v1/market/stock-analyses' && options.method === 'POST') {
                  return Promise.resolve({
                    analysis_id:'analysis-cancel', job_id:'job-cancel', status:'running',
                  });
                }
                if (path === '/api/v1/jobs/job-cancel/events') return Promise.resolve({items:[]});
                if (path === '/api/v1/jobs/job-cancel/cancel') {
                  return Promise.resolve({id:'job-cancel', status:'cancelled', phase:'服务端确认取消'});
                }
                if (path === '/api/v1/jobs/job-cancel') {
                  window.__cancelJobPolls += 1;
                  return Promise.resolve({id:'job-cancel', status:'running', progress:20, phase:'运行中'});
                }
                return nativeApi(input, options);
              };
            }"""
        )
        page.locator("#stock-analysis-query").fill("600519.SH")
        page.locator("#stock-analysis-form button.primary").click()
        page.locator("#stock-analysis-current-phase").get_by_text("运行中", exact=True).wait_for()
        page.locator("#stock-analysis-cancel").click()
        page.get_by_text("服务端确认取消", exact=True).wait_for()
        polls = page.evaluate("window.__cancelJobPolls")
        page.wait_for_timeout(1_000)

        active = page.evaluate("JSON.parse(localStorage.getItem('qm.stock-analysis.active.v2'))")
        assert active["status"] == "cancelled"
        assert active["phase"] == "服务端确认取消"
        assert page.evaluate("window.__cancelJobPolls") == polls
        browser.close()


def test_stock_analysis_slow_cancel_cannot_regress_terminal_poll(live_server):
    url, _ = live_server
    with playwright_sync.sync_playwright() as manager:
        browser = manager.chromium.launch()
        page = browser.new_page(viewport={"width": 1280, "height": 900})
        page.goto(f"{url}/#today/stock-analysis")
        page.locator("#stock-analysis-query").wait_for(state="visible")
        page.evaluate(
            """() => {
              const nativeApi = window.QuantMasterAPI;
              let jobCalls = 0;
              window.__terminalCancelPending = false;
              window.__resolveTerminalCancel = null;
              window.QuantMasterAPI = (input, options = {}) => {
                const path = new URL(input, location.href).pathname;
                if (path === '/api/v1/market/stock-analyses' && options.method === 'POST') {
                  return Promise.resolve({
                    analysis_id:'analysis-terminal', job_id:'job-terminal', status:'running',
                  });
                }
                if (path === '/api/v1/jobs/job-terminal/events') return Promise.resolve({items:[]});
                if (path === '/api/v1/jobs/job-terminal/cancel') {
                  window.__terminalCancelPending = true;
                  return new Promise(resolve => {
                    window.__resolveTerminalCancel = () => resolve({
                      id:'job-terminal', status:'cancelling', phase:'旧取消响应',
                    });
                  });
                }
                if (path === '/api/v1/jobs/job-terminal') {
                  jobCalls += 1;
                  return Promise.resolve(jobCalls === 1
                    ? {id:'job-terminal', status:'running', progress:20, phase:'运行中'}
                    : {id:'job-terminal', status:'completed', progress:100, phase:'任务已经完成'});
                }
                if (path === '/api/v1/market/stock-analyses/analysis-terminal') {
                  return Promise.resolve({analysis_id:'analysis-terminal', status:'completed'});
                }
                return nativeApi(input, options);
              };
            }"""
        )
        page.locator("#stock-analysis-query").fill("600519.SH")
        page.locator("#stock-analysis-form button.primary").click()
        page.locator("#stock-analysis-current-phase").get_by_text("运行中", exact=True).wait_for()
        page.locator("#stock-analysis-cancel").click()
        page.wait_for_function("() => window.__terminalCancelPending === true")
        page.get_by_text("任务已经完成", exact=True).wait_for()
        page.evaluate("window.__resolveTerminalCancel()")
        page.wait_for_timeout(100)

        active = page.evaluate("JSON.parse(localStorage.getItem('qm.stock-analysis.active.v2'))")
        assert active["status"] == "completed"
        assert active["phase"] == "任务已经完成"
        browser.close()


def test_settings_remount_resumes_pending_autosave_and_active_data_poll(live_server):
    url, _ = live_server
    data_polls = {"count": 0}
    saves = {"count": 0}

    def route_api(route):
        request = route.request
        path = request.url.split("?", 1)[0]
        if path.endswith("/api/v1/jobs") and "domain=data" in request.url:
            route.fulfill(json={"items": [{
                "id": "data-active", "status": "running", "progress": 10,
                "next_index": 1, "total": 10, "succeeded": 1, "failures": [],
            }]})
        elif path.endswith("/api/v1/jobs/data-active"):
            data_polls["count"] += 1
            route.fulfill(json={
                "id": "data-active", "status": "running", "progress": 10,
                "next_index": 1, "total": 10, "succeeded": 1, "failures": [],
            })
        elif path.endswith("/api/v1/settings/validate"):
            route.fulfill(json={"normalized": request.post_data_json})
        elif path.endswith("/api/v1/settings") and request.method == "PUT":
            saves["count"] += 1
            route.fulfill(json={
                "status": "ok", "settings": request.post_data_json, "runtime": {},
                "warnings": [], "changed_fields": [], "restart_required": [],
                "apply_status": {}, "persisted_revision": saves["count"],
            })
        else:
            route.fallback()

    with playwright_sync.sync_playwright() as manager:
        browser = manager.chromium.launch()
        page = browser.new_page(viewport={"width": 1280, "height": 900})
        page.route("**/api/v1/**", route_api)
        page.goto(f"{url}/#runtime/settings")
        page.locator("#settings-config-path").wait_for(state="visible")
        page.wait_for_timeout(1_000)
        assert data_polls["count"] >= 1

        page.locator('[data-settings-section="server"]').click()
        port = page.locator('[name="server.port"]')
        next_port = str(int(port.input_value()) + 1)
        port.evaluate(
            """(input, value) => {
              input.value = value;
              input.dispatchEvent(new Event('input', {bubbles:true}));
              document.querySelector('header [data-workspace="account"]').click();
            }""",
            next_port,
        )
        page.wait_for_url(re.compile(r"#account/paper$"))
        paused_polls = data_polls["count"]
        page.wait_for_timeout(1_000)
        assert data_polls["count"] == paused_polls
        assert saves["count"] == 0

        page.get_by_role("button", name="设置", exact=True).click()
        page.wait_for_url(re.compile(r"#runtime/settings$"))
        page.wait_for_timeout(1_000)
        assert data_polls["count"] > paused_polls
        assert saves["count"] == 1
        browser.close()


def test_settings_old_data_poll_cannot_overwrite_remounted_terminal_state(live_server):
    url, _ = live_server
    with playwright_sync.sync_playwright() as manager:
        browser = manager.chromium.launch()
        page = browser.new_page(viewport={"width": 1280, "height": 900})
        page.goto(f"{url}/#runtime/settings")
        page.locator("#settings-config-path").wait_for(state="visible")
        page.locator('[data-settings-section="local-data"]').click()
        page.evaluate(
            """() => {
              const nativeApi = window.QuantMasterAPI;
              window.__dataPolls = 0;
              window.__dataPollPending = false;
              window.__resolveOldDataPoll = null;
              const running = {
                id:'data-race', status:'running', progress:20, next_index:1,
                total:2, succeeded:1, failures:[], current_symbol:'旧响应',
              };
              window.QuantMasterAPI = (input, options = {}) => {
                const method = String(options.method || 'GET').toUpperCase();
                const url = new URL(input, location.href);
                if (url.pathname === '/api/v1/data/refresh/preview' && method === 'POST') {
                  return Promise.resolve({
                    message:'同步 2 个标的', start:'2026-08-01', end:'2026-08-15',
                    total:2, unhealthy_sources:[],
                  });
                }
                if (url.pathname === '/api/v1/data/refresh' && method === 'POST') {
                  return Promise.resolve(running);
                }
                if (url.pathname === '/api/v1/jobs' && url.searchParams.get('domain') === 'data') {
                  return Promise.resolve({items:[]});
                }
                if (url.pathname === '/api/v1/jobs/data-race') {
                  window.__dataPolls += 1;
                  if (window.__dataPolls === 1) {
                    window.__dataPollPending = true;
                    return new Promise(resolve => {
                      window.__resolveOldDataPoll = () => resolve(running);
                    });
                  }
                  return Promise.resolve({
                    id:'data-race', status:'completed', progress:100, next_index:2,
                    total:2, succeeded:2, failures:[], current_symbol:'新结果',
                  });
                }
                return nativeApi(input, options);
              };
            }"""
        )
        page.on("dialog", lambda dialog: dialog.accept())
        page.locator("#data-refresh-preview").click()
        page.locator("#data-refresh-start-button").click()
        page.wait_for_function("() => window.__dataPollPending === true")

        page.get_by_role("button", name="账户", exact=True).click()
        page.wait_for_url(re.compile(r"#account/paper$"))
        page.get_by_role("button", name="设置", exact=True).click()
        page.wait_for_url(re.compile(r"#runtime/settings$"))
        page.wait_for_function("() => window.__dataPolls === 2")
        page.get_by_text("增量同步完成", exact=False).wait_for()

        page.evaluate("window.__resolveOldDataPoll()")
        page.wait_for_timeout(1_000)
        assert page.evaluate("window.__dataPolls") == 2
        assert "增量同步完成" in page.locator("[data-refresh-phase]").inner_text()
        browser.close()


def test_settings_remount_resumes_stockdb_and_ignores_old_inflight_poll(live_server):
    url, _ = live_server
    with playwright_sync.sync_playwright() as manager:
        browser = manager.chromium.launch()
        page = browser.new_page(viewport={"width": 1280, "height": 900})
        page.goto(f"{url}/#runtime/settings")
        page.locator("#settings-config-path").wait_for(state="visible")
        page.locator('[data-settings-section="local-data"]').click()
        page.evaluate(
            """() => {
              const nativeApi = window.QuantMasterAPI;
              window.__freeStockDbCalls = 0;
              window.__freeStockDbPending = false;
              window.__resolveOldStockDb = null;
              window.QuantMasterAPI = (input, options = {}) => {
                const method = String(options.method || 'GET').toUpperCase();
                const path = new URL(input, location.href).pathname;
                if (path === '/api/v1/settings/free-stockdb/update' && method === 'POST') {
                  return Promise.resolve({state:'updating', phase:'syncing', message:'更新已开始'});
                }
                if (path === '/api/v1/settings/free-stockdb' && method === 'GET') {
                  window.__freeStockDbCalls += 1;
                  if (window.__freeStockDbCalls === 1) {
                    window.__freeStockDbPending = true;
                    return new Promise(resolve => {
                      window.__resolveOldStockDb = () => resolve({
                        state:'updating', phase:'syncing', message:'旧轮询仍在更新',
                      });
                    });
                  }
                  return Promise.resolve({state:'running', phase:'serving', message:'最新状态已完成'});
                }
                return nativeApi(input, options);
              };
            }"""
        )
        page.locator("#free-stockdb-update-now").click()
        page.wait_for_function("() => window.__freeStockDbPending === true")

        page.get_by_role("button", name="账户", exact=True).click()
        page.wait_for_url(re.compile(r"#account/paper$"))
        page.get_by_role("button", name="设置", exact=True).click()
        page.wait_for_url(re.compile(r"#runtime/settings$"))
        page.wait_for_function("() => window.__freeStockDbCalls >= 2")
        page.get_by_text("最新状态已完成", exact=False).wait_for()

        page.evaluate("window.__resolveOldStockDb()")
        page.wait_for_timeout(1_200)
        assert page.evaluate("window.__freeStockDbCalls") == 2
        assert "最新状态已完成" in page.locator("#free-stockdb-sidecar-status").inner_text()
        browser.close()


def test_settings_remount_resumes_weixin_login_without_old_poll_rearm(live_server):
    url, _ = live_server
    with playwright_sync.sync_playwright() as manager:
        browser = manager.chromium.launch()
        page = browser.new_page(viewport={"width": 1280, "height": 900})
        page.goto(f"{url}/#runtime/settings")
        config_path = page.locator("#settings-config-path")
        config_path.wait_for(state="visible")
        playwright_sync.expect(config_path).not_to_have_text("正在读取配置…")
        page.locator('[data-settings-section="automation"]').click()
        playwright_sync.expect(page.locator("#weixin-login-start")).to_be_enabled()
        page.evaluate(
            """() => {
              const nativeApi = window.QuantMasterAPI;
              window.__weixinCalls = 0;
              window.__weixinPending = false;
              window.__resolveOldWeixin = null;
              window.QuantMasterAPI = (input, options = {}) => {
                const method = String(options.method || 'GET').toUpperCase();
                const path = new URL(input, location.href).pathname;
                if (path === '/api/v1/automation/channels/weixin/login' && method === 'POST') {
                  return Promise.resolve({
                    session_id:'wx-active', qrcode_url:'data:image/svg+xml,%3Csvg/%3E',
                  });
                }
                if (path === '/api/v1/automation/channels/weixin/login/wx-active' && method === 'GET') {
                  window.__weixinCalls += 1;
                  if (window.__weixinCalls === 1) {
                    window.__weixinPending = true;
                    return new Promise(resolve => {
                      window.__resolveOldWeixin = () => resolve({status:'wait'});
                    });
                  }
                  return Promise.resolve({status:'expired'});
                }
                return nativeApi(input, options);
              };
            }"""
        )
        page.locator("#weixin-login-start").click()
        page.wait_for_function("() => window.__weixinPending === true")

        page.get_by_role("button", name="账户", exact=True).click()
        page.wait_for_url(re.compile(r"#account/paper$"))
        page.get_by_role("button", name="设置", exact=True).click()
        page.wait_for_url(re.compile(r"#runtime/settings$"))
        page.wait_for_function("() => window.__weixinCalls >= 2")
        page.get_by_text("二维码已失效，请重新生成", exact=True).wait_for()

        page.evaluate("window.__resolveOldWeixin()")
        page.wait_for_timeout(900)
        assert page.evaluate("window.__weixinCalls") == 2
        assert page.locator("#weixin-login-start").is_enabled()
        assert page.locator("#weixin-login-status").inner_text() == "二维码已失效，请重新生成"
        browser.close()


def test_settings_weixin_create_response_survives_navigation_before_session_id(live_server):
    url, _ = live_server
    with playwright_sync.sync_playwright() as manager:
        browser = manager.chromium.launch()
        page = browser.new_page(viewport={"width": 1280, "height": 900})
        page.goto(f"{url}/#runtime/settings")
        config_path = page.locator("#settings-config-path")
        config_path.wait_for(state="visible")
        playwright_sync.expect(config_path).not_to_have_text("正在读取配置…")
        page.locator('[data-settings-section="automation"]').click()
        playwright_sync.expect(page.locator("#weixin-login-start")).to_be_enabled()
        page.evaluate(
            """() => {
              const nativeApi = window.QuantMasterAPI;
              window.__weixinCreatePending = false;
              window.__weixinCreateRequests = 0;
              window.__weixinCreatePolls = 0;
              window.__resolveWeixinCreate = null;
              window.QuantMasterAPI = (input, options = {}) => {
                const path = new URL(input, location.href).pathname;
                if (path === '/api/v1/automation/channels/weixin/login' && options.method === 'POST') {
                  window.__weixinCreateRequests += 1;
                  window.__weixinCreatePending = true;
                  return new Promise(resolve => {
                    window.__resolveWeixinCreate = () => resolve({
                      session_id:'wx-created', qrcode_url:'data:image/svg+xml,%3Csvg/%3E',
                    });
                  });
                }
                if (path === '/api/v1/automation/channels/weixin/login/wx-created') {
                  window.__weixinCreatePolls += 1;
                  return Promise.resolve({status:'expired'});
                }
                return nativeApi(input, options);
              };
            }"""
        )
        page.locator("#weixin-login-start").click()
        page.wait_for_function("() => window.__weixinCreatePending === true")

        page.get_by_role("button", name="账户", exact=True).click()
        page.wait_for_url(re.compile(r"#account/paper$"))
        page.get_by_role("button", name="设置", exact=True).click()
        page.wait_for_url(re.compile(r"#runtime/settings$"))
        assert page.locator("#weixin-login-start").is_disabled()
        page.locator("#weixin-login-start").dispatch_event("click")
        assert page.evaluate("window.__weixinCreateRequests") == 1

        page.get_by_role("button", name="账户", exact=True).click()
        page.wait_for_url(re.compile(r"#account/paper$"))
        page.evaluate("window.__resolveWeixinCreate()")
        page.wait_for_timeout(900)
        assert page.evaluate("window.__weixinCreatePolls") == 0

        page.get_by_role("button", name="设置", exact=True).click()
        page.wait_for_url(re.compile(r"#runtime/settings$"))
        page.wait_for_function("() => window.__weixinCreatePolls === 1")
        page.get_by_text("二维码已失效，请重新生成", exact=True).wait_for()
        assert page.locator("#weixin-login-start").is_enabled()
        browser.close()


def test_settings_weixin_create_rejection_off_workspace_allows_remount_retry(live_server):
    url, _ = live_server
    with playwright_sync.sync_playwright() as manager:
        browser = manager.chromium.launch()
        page = browser.new_page(viewport={"width": 1280, "height": 900})
        page.goto(f"{url}/#runtime/settings")
        config_path = page.locator("#settings-config-path")
        config_path.wait_for(state="visible")
        playwright_sync.expect(config_path).not_to_have_text("正在读取配置…")
        page.locator('[data-settings-section="automation"]').click()
        playwright_sync.expect(page.locator("#weixin-login-start")).to_be_enabled()
        page.evaluate(
            """() => {
              const nativeApi = window.QuantMasterAPI;
              window.__weixinRejectPending = false;
              window.__weixinCreateAttempts = 0;
              window.__rejectWeixinCreate = null;
              window.QuantMasterAPI = (input, options = {}) => {
                const path = new URL(input, location.href).pathname;
                if (path === '/api/v1/automation/channels/weixin/login' && options.method === 'POST') {
                  window.__weixinCreateAttempts += 1;
                  if (window.__weixinCreateAttempts === 1) {
                    window.__weixinRejectPending = true;
                    return new Promise((_, reject) => {
                      window.__rejectWeixinCreate = () => reject(new Error('二维码服务暂时不可用'));
                    });
                  }
                  return Promise.resolve({
                    session_id:'wx-retry', qrcode_url:'data:image/svg+xml,%3Csvg/%3E',
                  });
                }
                if (path === '/api/v1/automation/channels/weixin/login/wx-retry') {
                  return Promise.resolve({status:'expired'});
                }
                return nativeApi(input, options);
              };
            }"""
        )
        page.locator("#weixin-login-start").click()
        page.wait_for_function("() => window.__weixinRejectPending === true")

        page.get_by_role("button", name="账户", exact=True).click()
        page.wait_for_url(re.compile(r"#account/paper$"))
        page.evaluate("window.__rejectWeixinCreate()")
        page.wait_for_timeout(100)
        assert page.locator("#weixin-login-status").inner_text() == "正在申请二维码…"

        page.get_by_role("button", name="设置", exact=True).click()
        page.wait_for_url(re.compile(r"#runtime/settings$"))
        assert page.locator("#weixin-login-start").is_enabled()
        page.locator("#weixin-login-start").click()
        page.get_by_text("二维码已失效，请重新生成", exact=True).wait_for()
        assert page.evaluate("window.__weixinCreateAttempts") == 2
        assert page.locator("#weixin-login-start").is_enabled()
        browser.close()


def test_settings_remount_resumes_only_active_research_and_migrations(live_server):
    url, _ = live_server
    polls = {"research": 0, "migration": 0, "contract": 0}
    active = {
        "research": {
            "id": "research-active", "status": "running", "progress": 20,
            "next_index": 1, "total": 5, "succeeded": 1, "failures": [],
        },
        "migration": {
            "id": "migration-active", "status": "running", "progress": 20,
            "phase": "复制数据",
        },
        "contract": {
            "id": "contract-active", "status": "running", "domain": "decision",
            "phase": "扫描", "total": 10, "checked": 2, "converted": 0,
            "blank": 0, "review": 0, "conflicts": 0, "unknown_results": [],
        },
    }

    def route_api(route):
        request = route.request
        path = request.url.split("?", 1)[0]
        if path.endswith("/api/v1/research/data/catalog"):
            route.fulfill(json={"datasets": [], "specs": []})
        elif path.endswith("/api/v1/research/data/capabilities"):
            route.fulfill(json={"data": [], "kernel": {"backend": "python"}})
        elif path.endswith("/api/v1/research/data/jobs"):
            route.fulfill(json={"items": [active["research"]]})
        elif path.endswith("/api/v1/research/data/jobs/research-active"):
            polls["research"] += 1
            route.fulfill(json=active["research"])
        elif path.endswith("/api/v1/data/migrations") and request.method == "POST":
            route.fulfill(json=active["migration"])
        elif path.endswith("/api/v1/data/migrations/migration-active"):
            polls["migration"] += 1
            route.fulfill(json=active["migration"])
        elif path.endswith("/api/v1/data/contract-migrations"):
            route.fulfill(json={
                "available_types": ["decision"], "latest": active["contract"],
            })
        elif path.endswith("/api/v1/data/contract-migrations/contract-active"):
            polls["contract"] += 1
            route.fulfill(json=active["contract"])
        else:
            route.fallback()

    with playwright_sync.sync_playwright() as manager:
        browser = manager.chromium.launch()
        page = browser.new_page(viewport={"width": 1280, "height": 900})
        page.route("**/api/v1/**", route_api)
        page.goto(f"{url}/#runtime/settings")
        page.locator("#settings-config-path").wait_for(state="visible")

        page.locator('[data-settings-section="research-data"]').click()
        page.locator("#research-artifacts > summary").click()
        page.get_by_text("研究生产中", exact=False).wait_for()
        page.locator('[data-settings-section="local-data"]').click()
        page.locator("#migration-target").fill("C:/QuantMaster-migration-test")
        page.locator("#migration-start").click()
        page.wait_for_timeout(1_000)
        assert all(count >= 1 for count in polls.values()), polls

        page.get_by_role("button", name="账户", exact=True).click()
        page.wait_for_url(re.compile(r"#account/paper$"))
        page.wait_for_timeout(300)
        paused = dict(polls)
        page.wait_for_timeout(2_100)
        assert polls == paused

        page.get_by_role("button", name="设置", exact=True).click()
        page.wait_for_url(re.compile(r"#runtime/settings$"))
        page.wait_for_timeout(2_100)
        assert all(polls[name] > paused[name] for name in polls), {"before": paused, "after": polls}
        browser.close()
