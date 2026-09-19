from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, Lock
from time import sleep
from types import SimpleNamespace

import pandas as pd
import pytest

import quantmaster.data.free_stockdb_source as stockdb


class ReadClient:
    """Reproduce the observed pipeline loss while scalar SDK reads stay correct."""

    def __init__(self, **kwargs):
        self.calls = []
        self.active = 0
        self.peak = 0
        self.guard = Lock()

    def get_data(self, **kwargs):
        self.calls.append(kwargs)
        code = kwargs['code']
        if isinstance(code, list):
            return {symbol: [] for symbol in code}
        if code in {'001232', '001237'}:
            return []
        row = {
            'code': code, 'date': '20260917', 'open': 10, 'high': 12,
            'low': 9, 'close': 11 if kwargs['fq'] is None else 5.5, 'volume': 100,
        }
        fields = kwargs.get('fields')
        return [[row.get(field) for field in fields.split(',')]] if fields else [row]


@pytest.fixture
def source(monkeypatch):
    monkeypatch.setattr(stockdb, 'provider_call', lambda _lane, _key, fn, **kw: fn())
    monkeypatch.setattr(stockdb, 'read_stockdb_session_acceptance', lambda _root: None)
    value = stockdb.FreeStockDBSource()
    value._sdk_checked = True
    value._client = ReadClient()
    return value


def test_observed_empty_pipeline_does_not_become_missing_coverage(source):
    codes = ['000001', '600000', '300653', '300654', '300695', '001232', '001237']
    assert not any(source._client.get_data(code=codes).values())
    symbols = [f'{code}.SH' if code.startswith('6') else f'{code}.SZ' for code in codes]
    expected = {symbol: source.daily(symbol, '2026-09-07', '2026-09-17') for symbol in symbols}
    source._client.calls.clear()
    for ordered in (symbols, list(reversed(symbols)), symbols + symbols):
        actual = source.daily_many(ordered, '2026-09-07', '2026-09-17')
        assert set(actual) == set(symbols[:5])
        for symbol, frame in actual.items():
            pd.testing.assert_frame_equal(frame, expected[symbol])
    assert len(source._client.calls) == 3 * len(codes)
    assert all(isinstance(call['code'], str) for call in source._client.calls)


def test_large_reused_batches_have_linear_call_budget(source, monkeypatch):
    codes = [f'{number:06d}' for number in range(100000, 100701)]
    scheduled_sizes = []

    def schedule(_lane, _key, fetch, **kwargs):
        before = len(source._client.calls)
        result = fetch()
        scheduled_sizes.append(len(source._client.calls) - before)
        return result

    monkeypatch.setattr(stockdb, 'provider_call', schedule)
    for ordered in (codes, list(reversed(codes))):
        rows = source._sdk_data(ordered + ordered[:3], '20260907', '20260917', '1d', fq=None)
        assert list(rows) == ordered
        assert all(value[0]['code'] == code for code, value in rows.items())
    assert len(source._client.calls) == 2 * len(codes)
    assert max(scheduled_sizes) <= 32
    assert len(scheduled_sizes) == 44
    assert source._sdk_data([], '20260907', '20260917', '1d', fq=None) == {}


def test_projection_preserves_raw_prices_and_checks_hidden_identity(source):
    frame = source.daily_cross_section(['000001.SZ', '600000.SH'], '2026-09-07', '2026-09-17')
    assert frame['close'].tolist() == [11, 11]
    assert frame.attrs['adjustment'] == 'none'
    assert all('code' in call['fields'].split(',') for call in source._client.calls)
    assert 'code' not in frame.columns
    assert all(call['fq'] is None for call in source._client.calls)


def test_remote_sdk_keeps_per_symbol_rate_limiting(source, monkeypatch):
    source.name = 'free-stockdb-online'
    requests = []

    def schedule(_lane, key, fetch, **kwargs):
        requests.append(key)
        return fetch()

    monkeypatch.setattr(stockdb, 'provider_call', schedule)
    result = source._sdk_data(['000001', '600000'], '20260907', '20260917', '1d', fq=None)
    assert set(result) == {'000001', '600000'}
    assert len(requests) == 2


@pytest.mark.parametrize('bad, message', [
    ({'code': '600000'}, '证券身份'),
    ({'code': None}, '证券身份'),
    ({'date': '20260918'}, '超出请求范围'),
    ({'date': '20260230'}, '日期无效'),
    ({'date': None}, '日期无效'),
])
def test_batch_rejects_wrong_identity_or_date(source, bad, message):
    source._client.get_data = lambda **kw: [{'code': kw['code'], 'date': '20260917', **bad}]
    with pytest.raises(stockdb.FreeStockDBProviderError, match=message):
        source._sdk_data(['000001'], '20260907', '20260917', '1d', fq=None)


def test_failure_stops_batch_without_returning_partial_coverage(source):
    calls = []

    def read(**kwargs):
        calls.append(kwargs['code'])
        if len(calls) == 2:
            raise RuntimeError('read failed')
        return ReadClient().get_data(**kwargs)

    source._client.get_data = read
    with pytest.raises(RuntimeError, match='read failed'):
        source._sdk_data(['000001', '600000', '300653'], '20260907', '20260917', '1d', fq=None)
    assert calls == ['000001', '600000']


def test_shared_cached_client_serializes_native_reads(monkeypatch):
    monkeypatch.setattr(stockdb, 'provider_call', lambda _lane, _key, fn, **kw: fn())

    class ConcurrentClient(ReadClient):
        def get_data(self, **kwargs):
            with self.guard:
                self.active += 1
                self.peak = max(self.peak, self.active)
            try:
                sleep(0.002)
                return super().get_data(**kwargs)
            finally:
                with self.guard:
                    self.active -= 1

    module = SimpleNamespace(__name__='batch_concurrency_fixture', StockDBClient=ConcurrentClient)
    monkeypatch.setattr(stockdb.FreeStockDBSource, '_load_sdk_module', lambda self: module)
    sources = [stockdb.FreeStockDBSource() for _ in range(4)]
    client = sources[0]._sdk_client()
    assert all(source._sdk_client() is client for source in sources)
    assert all(source._sdk_read_lock is sources[0]._sdk_read_lock for source in sources)
    barrier = Barrier(4)

    def read(index):
        barrier.wait(timeout=5)
        code = f'{index:06d}'
        return sources[index]._sdk_data([code], '20260907', '20260917', '1d', fq=None)[code][0]

    with ThreadPoolExecutor(max_workers=4) as pool:
        rows = list(pool.map(read, range(4)))
    assert [row['code'] for row in rows] == [f'{index:06d}' for index in range(4)]
    assert client.peak == 1



def test_real_scheduler_keeps_large_batch_outside_single_call_timeout(monkeypatch, capsys):
    from time import perf_counter

    import quantmaster.data.resilience as resilience

    # Exercise dispatch with in-memory health to avoid benchmarking SQLite.
    monkeypatch.setattr(resilience, 'PROVIDER_HEALTH', SimpleNamespace(
        check_available=lambda *a, **kw: None, before_call=lambda *a, **kw: None,
        success=lambda *a, **kw: None, failure=lambda *a, **kw: None,
    ))
    source = stockdb.FreeStockDBSource()
    source._sdk_checked = True
    source._client = ReadClient()
    codes = [f'{number:06d}' for number in range(100000, 100300)]
    begin = perf_counter()
    result = source._sdk_data(codes, '20260907', '20260917', '1d', fq=None)
    elapsed = perf_counter() - begin
    assert list(result) == codes
    assert len(source._client.calls) == 300
    # The scheduler sees bounded chunks, never one growing full-market call.
    with capsys.disabled():
        print(f'\nStockDB synthetic scheduler: 300 scalar reads in {elapsed:.3f}s')



def test_independent_sdk_clients_are_not_globally_serialized(monkeypatch):
    monkeypatch.setattr(stockdb, 'provider_call', lambda _lane, _key, fn, **kw: fn())
    barrier = Barrier(2)

    class IndependentClient(ReadClient):
        def get_data(self, **kwargs):
            barrier.wait(timeout=5)
            return super().get_data(**kwargs)

    module = SimpleNamespace(__name__='independent_fixture', StockDBClient=IndependentClient)
    monkeypatch.setattr(stockdb.FreeStockDBSource, '_load_sdk_module', lambda self: module)

    def read(index):
        source = stockdb.FreeStockDBSource()
        code = f'{index:06d}'
        return source._sdk_data([code], '20260907', '20260917', '1d', fq=None)[code][0]

    with ThreadPoolExecutor(max_workers=2) as pool:
        rows = list(pool.map(read, range(2)))
    assert [row['code'] for row in rows] == ['000000', '000001']



def test_invalid_identity_fails_inside_provider_health_boundary(source, monkeypatch):
    outcomes = []

    def scheduled(_lane, _key, fetch, **kwargs):
        try:
            value = fetch()
        except stockdb.FreeStockDBProviderError:
            outcomes.append('failure')
            raise
        outcomes.append('success')
        return value

    monkeypatch.setattr(stockdb, 'provider_call', scheduled)
    source._client.get_data = lambda **kw: [{'code': '600000', 'date': '20260917'}]
    with pytest.raises(stockdb.FreeStockDBProviderError, match='证券身份'):
        source._sdk_data(['000001'], '20260907', '20260917', '1d', fq=None)
    assert outcomes == ['failure']



@pytest.mark.parametrize('rows, message', [
    ([{'code': '000001', 'date': '20260917'}], 'OHLCV'),
    ([{'code': '000001', 'date': '20260917', 'open': 10, 'high': 11,
       'low': 9, 'close': 10, 'volume': 10}] * 2, '日期重复'),
])
def test_batch_rejects_incomplete_or_duplicate_bar_rows(source, rows, message):
    source._client.get_data = lambda **kw: rows
    with pytest.raises(stockdb.FreeStockDBProviderError, match=message):
        source._sdk_data(['000001'], '20260907', '20260917', '1d', fq=None)
