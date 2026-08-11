from __future__ import annotations

import time

from quantmaster.runtime.metrics import RuntimeMetrics


def test_runtime_metrics_records_requests_and_node_attribution(tmp_path):
    metrics = RuntimeMetrics(tmp_path / "metrics.sqlite")
    metrics.record_request(
        route="/api/v1/rotation/themes", method="GET", status_code=200,
        duration_ms=12.5, response_bytes=512,
    )
    with metrics.node_timer("rotation.themes", input_fingerprint="abc") as dimensions:
        dimensions.update(cache_hit=True, input_rows=10, output_rows=3, files_read=0)
        time.sleep(0.001)
    with metrics._conn() as connection:
        request = connection.execute("SELECT * FROM request_metrics").fetchone()
        node = connection.execute("SELECT * FROM refresh_node_metrics").fetchone()
    assert request["route"] == "/api/v1/rotation/themes"
    assert request["response_bytes"] == 512
    assert node["node"] == "rotation.themes"
    assert node["cache_hit"] == 1
    assert node["output_rows"] == 3
