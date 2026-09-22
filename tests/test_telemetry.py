"""Telemetry ingest & shadow merge tests: order, duplicates, skew, quota,
concurrent writes, and auditable merge decisions."""
import threading

from app.telemetry import TelemetryStore


def test_out_of_order_messages_never_roll_back_state():
    store = TelemetryStore(clock=lambda: 10_000.0)
    # newest first, then older ones arrive late (worst-case reordering)
    r_new = store.ingest("d1", "m3", seq=3, device_ts=9000.0, payload={"temp": 30})
    r_old = store.ingest("d1", "m2", seq=2, device_ts=8000.0, payload={"temp": 20})
    r_oldest = store.ingest("d1", "m1", seq=1, device_ts=7000.0, payload={"temp": 10})
    print(f"[ooo] decisions old={[d.outcome for d in r_old.decisions]} "
          f"shadow={store.shadow('d1')} versions={store.field_versions('d1')}")
    assert r_new.decisions[0].outcome == "applied"
    assert r_old.decisions[0].outcome == "stale_rejected"
    assert r_oldest.decisions[0].outcome == "stale_rejected"
    assert store.shadow("d1")["temp"] == 30  # no regression


def test_time_shifted_message_does_not_overwrite_newer_state():
    store = TelemetryStore(clock=lambda: 10_000.0)
    store.ingest("d1", "m1", seq=1, device_ts=9500.0, payload={"mode": "auto"})
    # a device with a lagging clock reports an older timestamp
    r = store.ingest("d1", "m2", seq=2, device_ts=1000.0, payload={"mode": "manual"})
    print(f"[skew] decision={r.decisions[0].outcome} reason={r.decisions[0].reason}")
    assert r.decisions[0].outcome == "stale_rejected"
    assert store.shadow("d1")["mode"] == "auto"


def test_duplicate_message_is_idempotent_and_not_double_counted():
    store = TelemetryStore(clock=lambda: 10_000.0)
    store.ingest("d1", "m1", seq=1, device_ts=9000.0, payload={"count": 1})
    dup = store.ingest("d1", "m1", seq=1, device_ts=9000.0, payload={"count": 1})
    print(f"[dup] code={dup.code} accepted_count={store.accepted_count('d1')}")
    assert dup.accepted and dup.code == "duplicate_message"
    assert store.accepted_count("d1") == 1
    assert store.shadow("d1")["count"] == 1


def test_per_field_merge_keeps_newest_of_each_field():
    store = TelemetryStore(clock=lambda: 10_000.0)
    store.ingest("d1", "m1", seq=5, device_ts=9000.0,
                 payload={"temp": 30, "hum": 50})
    # stale temp but genuinely newer hum in one message
    r = store.ingest("d1", "m2", seq=6, device_ts=9500.0,
                     payload={"temp": 1, "hum": 60})
    outcomes = {d.field: d.outcome for d in r.decisions}
    print(f"[per-field] outcomes={outcomes} shadow={store.shadow('d1')}")
    assert store.shadow("d1") == {"temp": 1, "hum": 60}  # ts is newer -> both win
    # now a genuinely mixed case: older ts loses on both fields
    r2 = store.ingest("d1", "m3", seq=4, device_ts=8000.0,
                      payload={"temp": 99, "hum": 99})
    outcomes2 = {d.field: d.outcome for d in r2.decisions}
    print(f"[per-field2] outcomes={outcomes2} shadow={store.shadow('d1')}")
    assert outcomes2 == {"temp": "stale_rejected", "hum": "stale_rejected"}
    assert store.shadow("d1") == {"temp": 1, "hum": 60}


def test_quota_exceeded_is_reported_not_silently_dropped():
    store = TelemetryStore(per_device_quota=2, clock=lambda: 10_000.0)
    store.ingest("d1", "m1", seq=1, device_ts=1.0, payload={"a": 1})
    store.ingest("d1", "m2", seq=2, device_ts=2.0, payload={"a": 2})
    r = store.ingest("d1", "m3", seq=3, device_ts=3.0, payload={"a": 3})
    print(f"[quota] code={r.code} rejected_log={[x.code for x in store.rejected('d1')]}")
    assert not r.accepted and r.code == "quota_exceeded"
    assert [x.code for x in store.rejected("d1")] == ["quota_exceeded"]
    assert store.shadow("d1")["a"] == 2  # rejected message left no trace


def test_validation_failures_are_recorded():
    store = TelemetryStore(clock=lambda: 10_000.0, max_clock_skew_seconds=60)
    r1 = store.ingest("d1", "m1", seq=1, device_ts=999_999.0, payload={"a": 1})
    r2 = store.ingest("d1", "m2", seq=1, device_ts=1.0, payload={})
    r3 = store.ingest("d1", "m3", seq=1, device_ts=1.0, payload={"a": {"bad": "nested"}})
    codes = [x.code for x in store.rejected("d1")]
    print(f"[validation] codes={codes}")
    assert not r1.accepted and not r2.accepted and not r3.accepted
    assert codes == ["validation_failed"] * 3


def test_concurrent_ingest_is_consistent_and_counted_once():
    store = TelemetryStore(per_device_quota=10_000, clock=lambda: 10_000.0)
    threads_n, msgs_per_thread = 8, 50

    def worker(tid):
        for i in range(msgs_per_thread):
            # every message sent twice (retry storm) from multiple threads
            for _ in range(2):
                store.ingest("d1", f"t{tid}-m{i}", seq=tid * 1000 + i,
                             device_ts=9000.0 + tid, payload={f"f{tid}": i})

    threads = [threading.Thread(target=worker, args=(t,)) for t in range(threads_n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    count = store.accepted_count("d1")
    print(f"[concurrency] distinct={threads_n * msgs_per_thread} "
          f"accepted_count={count} shadow_fields={len(store.shadow('d1'))}")
    assert count == threads_n * msgs_per_thread  # duplicates not counted
    assert len(store.shadow("d1")) == threads_n
    for tid in range(threads_n):
        assert store.shadow("d1")[f"f{tid}"] == msgs_per_thread - 1
