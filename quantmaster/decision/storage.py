"""每日选股快照持久化，保证事后研究能还原当时真正看到的信号。"""

from __future__ import annotations

import hashlib
import json
import shutil
import sqlite3
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from quantmaster.config import get_config
from quantmaster.decision.schema import (
    DECISION_PAYLOAD_SCHEMA_VERSION,
    DecisionSchemaError,
    validate_current_payload,
)
from quantmaster.runtime.sqlite import connect_sqlite
from quantmaster.trading_sessions import daily_signal_cutoff

_MARKET_INPUT_CONTRACT = "decision-market-panel-v1"


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _json_sha256(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _frame_sha256(frame: pd.DataFrame) -> str:
    """Hash the semantic frame, independent of Parquet encoder metadata."""
    descriptor = {
        "columns": [str(value) for value in frame.columns],
        "column_names": [str(value or "") for value in frame.columns.names],
        "dtypes": [str(value) for value in frame.dtypes],
        "index_names": [str(value or "") for value in frame.index.names],
        "index_dtype": str(frame.index.dtype),
        "shape": [int(frame.shape[0]), int(frame.shape[1])],
    }
    digest = hashlib.sha256(
        json.dumps(
            descriptor, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        ).encode("utf-8")
    )
    digest.update(pd.util.hash_pandas_object(frame.index, index=False).values.tobytes())
    digest.update(pd.util.hash_pandas_object(frame, index=False).values.tobytes())
    return digest.hexdigest()


class DecisionStore:
    def __init__(self, path: Path | None = None, *, read_only: bool = False):
        self.path = Path(path) if path else get_config().data_root / "decisions.sqlite"
        self.read_only = bool(read_only)
        if not self.read_only:
            self.path.parent.mkdir(parents=True, exist_ok=True)
        self.evidence_root = self.path.parent / "decision_evidence"
        if not self.read_only:
            self._migrate()

    def _conn(self) -> sqlite3.Connection:
        return connect_sqlite(
            self.path,
            timeout=0.25 if self.read_only else 30.0,
            read_only=self.read_only,
        )

    @staticmethod
    def _validate_panel_cutoff(
        panel: dict[str, pd.DataFrame],
        signal_date: str,
    ) -> None:
        try:
            cutoff = daily_signal_cutoff(signal_date)
        except ValueError as exc:
            raise ValueError("正式决策 signal_date 需要使用 YYYY-MM-DD 格式") from exc
        for name, frame in panel.items():
            if not isinstance(frame.index, pd.DatetimeIndex) or frame.index.hasnans:
                raise ValueError(f"正式决策输入 {name} 缺少可验证的时间索引")
            timestamps = frame.index
            if timestamps.tz is None:
                timestamps = timestamps.tz_localize("Asia/Shanghai")
            else:
                timestamps = timestamps.tz_convert("Asia/Shanghai")
            if timestamps.max() > cutoff:
                raise ValueError(
                    f"正式决策输入 {name} 包含信号日上海 15:00 后的数据"
                )

    def _migrate(self) -> None:
        with self._conn() as conn:
            exists = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='selection_snapshots'"
            ).fetchone()
            if not exists:
                conn.execute(
                    "CREATE TABLE selection_snapshots ("
                    "signal_date TEXT NOT NULL, universe TEXT NOT NULL, "
                    "horizon INTEGER NOT NULL, profile TEXT NOT NULL, "
                    "policy_hash TEXT NOT NULL, model_version TEXT NOT NULL, "
                    "payload TEXT NOT NULL, "
                    "created_at REAL NOT NULL, "
                    "PRIMARY KEY(signal_date,universe,horizon,profile,policy_hash))"
                )
                return
            columns = {
                row[1] for row in conn.execute("PRAGMA table_info(selection_snapshots)")
            }
            if {"profile", "policy_hash"} <= columns:
                return
            raise RuntimeError("正式决策历史表不符合当前契约，无法加载")

    def freeze_market_input(
        self,
        panel: dict[str, pd.DataFrame],
        *,
        signal_date: str,
    ) -> dict[str, Any]:
        """Persist the exact decision panel as a content-addressed immutable artifact."""
        if not panel or "close" not in panel:
            raise ValueError("正式决策行情证据必须包含 close 面板")
        invalid = [name for name, frame in panel.items() if not isinstance(frame, pd.DataFrame)]
        if invalid:
            raise TypeError("行情面板字段不是 DataFrame：" + "、".join(sorted(invalid)))
        self._validate_panel_cutoff(panel, signal_date)
        self.evidence_root.mkdir(parents=True, exist_ok=True)
        staging = self.evidence_root / f".staging-{uuid.uuid4().hex}"
        staging.mkdir()
        try:
            records: list[dict[str, Any]] = []
            for index, (name, frame) in enumerate(sorted(panel.items())):
                if frame.empty:
                    raise ValueError(f"正式决策行情字段 {name} 为空")
                filename = f"{index:02d}.parquet"
                target = staging / filename
                frame.to_parquet(target)
                restored = pd.read_parquet(target)
                original_hash = _frame_sha256(frame)
                restored_hash = _frame_sha256(restored)
                if restored_hash != original_hash:
                    raise RuntimeError(f"行情字段 {name} 写入后语义哈希不一致")
                records.append(
                    {
                        "name": str(name),
                        "file": filename,
                        "rows": len(restored),
                        "columns": [str(value) for value in restored.columns],
                        "frame_sha256": restored_hash,
                        "file_sha256": _file_sha256(target),
                    }
                )
            semantic_records = [
                {key: value for key, value in record.items() if key != "file_sha256"}
                for record in records
            ]
            logical = {"contract": _MARKET_INPUT_CONTRACT, "fields": semantic_records}
            content_hash = _json_sha256(logical)
            manifest = {
                "contract": _MARKET_INPUT_CONTRACT,
                "fields": records,
                "content_hash": content_hash,
                "created_at": datetime.now(UTC).isoformat(),
            }
            (staging / "manifest.json").write_text(
                json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2),
                encoding="utf-8",
            )
            target_root = self.evidence_root / content_hash
            if target_root.exists():
                self.load_market_input(
                    {"contract": _MARKET_INPUT_CONTRACT, "content_hash": content_hash}
                )
                shutil.rmtree(staging)
            else:
                staging.replace(target_root)
            return {
                "contract": _MARKET_INPUT_CONTRACT,
                "content_hash": content_hash,
                "fields": [record["name"] for record in records],
                "rows": {record["name"]: record["rows"] for record in records},
            }
        finally:
            if staging.exists():
                shutil.rmtree(staging, ignore_errors=True)

    def load_market_input(
        self, evidence: dict[str, Any],
    ) -> dict[str, pd.DataFrame]:
        """Restore and verify a frozen decision panel; never fall back to current bars."""
        if str(evidence.get("contract") or "") != _MARKET_INPUT_CONTRACT:
            raise RuntimeError("正式决策行情证据契约缺失或不受支持")
        content_hash = str(evidence.get("content_hash") or "")
        if len(content_hash) != 64 or any(ch not in "0123456789abcdef" for ch in content_hash):
            raise RuntimeError("正式决策行情证据哈希无效")
        root = self.evidence_root / content_hash
        manifest_path = root / "manifest.json"
        if not manifest_path.is_file():
            raise RuntimeError("正式决策行情冻结制品缺失")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if (
            manifest.get("contract") != _MARKET_INPUT_CONTRACT
            or manifest.get("content_hash") != content_hash
        ):
            raise RuntimeError("正式决策行情 manifest 身份不一致")
        records = manifest.get("fields")
        if not isinstance(records, list) or not records:
            raise RuntimeError("正式决策行情 manifest 不完整")
        logical = {
            "contract": _MARKET_INPUT_CONTRACT,
            "fields": [
                {key: value for key, value in record.items() if key != "file_sha256"}
                for record in records
            ],
        }
        if _json_sha256(logical) != content_hash:
            raise RuntimeError("正式决策行情 manifest 已被改写")
        panel: dict[str, pd.DataFrame] = {}
        for record in records:
            name = str(record.get("name") or "")
            filename = str(record.get("file") or "")
            if not name or not filename or Path(filename).name != filename:
                raise RuntimeError("正式决策行情 manifest 路径无效")
            path = root / filename
            if not path.is_file() or _file_sha256(path) != record.get("file_sha256"):
                raise RuntimeError(f"正式决策行情字段 {name} 文件缺失或已改写")
            frame = pd.read_parquet(path)
            if (
                len(frame) != int(record.get("rows") or -1)
                or [str(value) for value in frame.columns] != record.get("columns")
                or _frame_sha256(frame) != record.get("frame_sha256")
            ):
                raise RuntimeError(f"正式决策行情字段 {name} 内容无法验证")
            panel[name] = frame
        if "close" not in panel:
            raise RuntimeError("正式决策行情冻结制品缺少 close")
        return panel

    def save(
        self,
        report: dict[str, Any],
        universe: str,
        *,
        panel: dict[str, pd.DataFrame] | None = None,
    ) -> None:
        """Append one formal decision identity; conflicting reruns fail closed."""
        mode = str(report.get("policy_mode") or "live")
        if mode == "retrospective":
            raise ValueError("retrospective 结果仅供事后研究，不能写入正式决策历史")
        signal_date = str(report.get("signal_date") or "")
        if mode == "historical_replay":
            if not signal_date or not report.get("generated_at"):
                raise ValueError("历史重放缺少 signal_date/generated_at 追溯字段")
            try:
                cutoff = daily_signal_cutoff(signal_date).astimezone(UTC)
            except ValueError as exc:
                raise ValueError("历史重放 signal_date 需要使用 YYYY-MM-DD 格式") from exc
            future_components = []
            invalid_components = []
            for item in (report.get("model_snapshot") or {}).get("components", []):
                deployed_at = str(item.get("deployed_at") or "")
                label = str(item.get("name") or item.get("version_id") or "未知组件")
                if not deployed_at:
                    is_builtin_rule = (
                        str(item.get("scope") or "") == "builtin"
                        and str(item.get("kind") or "") == "rule"
                        and str(item.get("version_id") or "") == "swing-adaptive-v2"
                        and bool(str(item.get("content_hash") or ""))
                    )
                    if not is_builtin_rule:
                        invalid_components.append(f"{label}（缺少 deployed_at）")
                    continue
                try:
                    deployed = datetime.fromisoformat(deployed_at.replace("Z", "+00:00"))
                except ValueError:
                    deployed = None
                if deployed is None or deployed.tzinfo is None:
                    invalid_components.append(label)
                elif deployed.astimezone(UTC) > cutoff:
                    future_components.append(label)
            if invalid_components:
                raise ValueError(
                    "历史重放包含无法验证时区的部署时间：" + "、".join(invalid_components)
                )
            if future_components:
                raise ValueError(
                    "历史重放包含信号日上海 15:00 后部署的模型："
                    + "、".join(future_components)
                )
        if panel is not None:
            report["market_input_evidence"] = self.freeze_market_input(
                panel,
                signal_date=signal_date,
            )
        elif report.get("market_input_evidence"):
            restored = self.load_market_input(report["market_input_evidence"])
            self._validate_panel_cutoff(restored, signal_date)
        else:
            raise ValueError("正式决策必须冻结实际参与计算的行情面板")
        created_timestamp = time.time()
        report["decision_schema_version"] = DECISION_PAYLOAD_SCHEMA_VERSION
        report["universe"] = universe
        report["created_at"] = datetime.fromtimestamp(
            created_timestamp, tz=UTC,
        ).isoformat()
        report.setdefault("picks", [])
        report.setdefault("profile", None)
        report.setdefault("policy_hash", None)
        report.setdefault("model_version", None)
        key = (
            report["signal_date"], universe, report["holding_horizon_days"],
            report["profile"] or "", report["policy_hash"] or "",
        )
        validate_current_payload(
            report,
            row_identity={
                "signal_date": key[0], "universe": key[1], "horizon": key[2],
                "profile": report["profile"], "policy_hash": report["policy_hash"],
                "model_version": report["model_version"], "created_at": report["created_at"],
            },
        )
        payload = json.dumps(report, ensure_ascii=False, allow_nan=False)
        with self._conn() as conn:
            existing = conn.execute(
                "SELECT payload FROM selection_snapshots WHERE "
                "signal_date=? AND universe=? AND horizon=? AND profile=? AND policy_hash=?",
                key,
            ).fetchone()
            if existing is not None:
                previous = json.loads(existing[0])
                same_output = (
                    previous.get("picks") == report.get("picks")
                    and previous.get("position_state") == report.get("position_state")
                )
                if same_output:
                    return
                raise RuntimeError(
                    "同一信号日/模型已有不同输入或输出的正式决策；"
                    "为保护证据链拒绝覆盖，请保留原快照并使用新的模型/策略版本"
                )
            conn.execute(
                "INSERT INTO selection_snapshots "
                "(signal_date,universe,horizon,profile,policy_hash,model_version,"
                "payload,created_at) VALUES (?,?,?,?,?,?,?,?)",
                (
                    *key,
                    report["model_version"] or "",
                    payload, created_timestamp,
                ),
            )

    def history(
        self, universe: str | None = None, limit: int = 30,
        profile: str | None = None, horizon: int | None = None,
    ) -> list[dict[str, Any]]:
        if limit < 1:
            return []
        filters: list[str] = []
        values: list[Any] = []
        if universe:
            filters.append("universe=?")
            values.append(universe)
        if profile:
            filters.append("profile=?")
            values.append(profile)
        if horizon is not None:
            filters.append("horizon=?")
            values.append(int(horizon))
        query = (
            "SELECT signal_date,universe,horizon,profile,policy_hash,model_version,"
            "payload,created_at FROM selection_snapshots "
        )
        if filters:
            query += "WHERE " + " AND ".join(filters) + " "
        query += "ORDER BY signal_date DESC, created_at DESC LIMIT ?"
        values.append(limit)
        try:
            with self._conn() as conn:
                rows = conn.execute(query, tuple(values)).fetchall()
        except (FileNotFoundError, sqlite3.OperationalError):
            return []
        result: list[dict[str, Any]] = []
        for row in rows:
            try:
                report = json.loads(str(row[6]))
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                raise DecisionSchemaError(
                    "decision_payload_invalid_json",
                    f"决策快照 {row[0]}/{row[1]} payload 已损坏，不会尝试旧格式 decoder",
                ) from exc
            identity = {
                "signal_date": row[0], "universe": row[1], "horizon": row[2],
                "profile": row[3] or None, "policy_hash": row[4] or None,
                "model_version": row[5] or None,
                "created_at": datetime.fromtimestamp(float(row[7]), tz=UTC).isoformat(),
            }
            validate_current_payload(report, row_identity=identity)
            result.append(report)
        return result

    def latest(self, universe: str | None = None) -> dict[str, Any] | None:
        rows = self.history(universe=universe, limit=1)
        return rows[0] if rows else None
