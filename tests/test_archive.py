import datetime as dt
import io
import json

from archive_app import create_app
from archive_app.catalog import (
    MAX_SPAN_DAYS,
    STATUS_BLOCKED,
    STATUS_COMPLETE,
    STATUS_FAILED,
    STATUS_PENDING,
    Event,
    MockCatalogSource,
    Store,
)
from archive_app.collector import collect, contiguous_segments
from archive_app.csv_import import CsvValidationError, parse_csv, verify_includes_collected


def write_catalog(tmp_path, events):
    path = tmp_path / "catalog.json"
    path.write_text(json.dumps({"events": events}), encoding="utf-8")
    return str(path)


def ev(eid, day, hh=0, mm=0, mag=3.0):
    return {"id": eid, "time": f"{day}T{hh:02d}:{mm:02d}:00Z", "magnitude": mag}


def make_store(tmp_path):
    store = Store(str(tmp_path / "archive.db"))
    store.init()
    return store


def statuses(store, run_id):
    return {row["day"]: row["status"] for row in store.days(run_id)}


def test_contiguous_segments():
    d = dt.date(2024, 1, 1)
    segs = contiguous_segments([d, d + dt.timedelta(days=1), d + dt.timedelta(days=5)])
    assert len(segs) == 2
    assert len(segs[0]) == 2 and len(segs[1]) == 1


def test_under_full_range_marks_every_day_complete_including_empty(tmp_path):
    path = write_catalog(tmp_path, [ev("A", "2024-01-02"), ev("B", "2024-01-03")])
    store = make_store(tmp_path)
    run = store.create_run(dt.date(2024, 1, 1), dt.date(2024, 1, 5))
    summary = collect(store, MockCatalogSource(path), run)
    assert summary.completed == 5
    assert all(v == STATUS_COMPLETE for v in statuses(store, run).values())
    assert [r["event_id"] for r in store.events(run)] == ["A", "B"]


def test_full_page_splits_until_exhausted(tmp_path):
    # 25 events inside a 3-day range: the whole range is full (20), splitting
    # yields a 1-day + 2-day pair that are both under the limit.
    events = [ev(f"D1-{i}", "2024-01-01", i % 24, i) for i in range(8)]
    events += [ev(f"D2-{i}", "2024-01-02", i % 24, i) for i in range(9)]
    events += [ev(f"D3-{i}", "2024-01-03", i % 24, i) for i in range(8)]
    path = write_catalog(tmp_path, events)
    store = make_store(tmp_path)
    run = store.create_run(dt.date(2024, 1, 1), dt.date(2024, 1, 3))
    summary = collect(store, MockCatalogSource(path), run)
    assert summary.blocked == 0
    assert all(v == STATUS_COMPLETE for v in statuses(store, run).values())
    assert len(store.events(run)) == 25


def test_single_full_day_is_blocked_and_keeps_first_page(tmp_path):
    events = [ev(f"X{i:02d}", "2024-01-02", i % 24, i) for i in range(27)]
    path = write_catalog(tmp_path, events)
    store = make_store(tmp_path)
    run = store.create_run(dt.date(2024, 1, 1), dt.date(2024, 1, 3))
    collect(store, MockCatalogSource(path), run)
    st = statuses(store, run)
    assert st["2024-01-02"] == STATUS_BLOCKED
    assert st["2024-01-01"] == STATUS_COMPLETE
    assert st["2024-01-03"] == STATUS_COMPLETE
    assert len(store.events(run, "2024-01-02")) == 20


def test_failed_request_marks_days_and_retry_skips_complete_and_blocked(tmp_path):
    events = [ev("A", "2024-01-01")]
    events += [ev(f"X{i:02d}", "2024-01-02", i % 24, i) for i in range(25)]
    path = write_catalog(tmp_path, events)
    store = make_store(tmp_path)
    run = store.create_run(dt.date(2024, 1, 1), dt.date(2024, 1, 3))

    # The upstream rejects the whole window; none of its days can be considered
    # archived, so every non-complete day is marked for retry.
    summary = collect(store, MockCatalogSource(path, fail_dates=["2024-01-03"]), run)
    assert summary.failed == 3
    assert set(statuses(store, run).values()) == {STATUS_FAILED}

    # Retry: the full range is split, day 1/3 complete and day 2 is blocked.
    summary = collect(store, MockCatalogSource(path), run)
    assert summary.failed == 0
    st = statuses(store, run)
    assert st["2024-01-01"] == STATUS_COMPLETE
    assert st["2024-01-02"] == STATUS_BLOCKED
    assert st["2024-01-03"] == STATUS_COMPLETE

    # A further retry must not request anything: complete and blocked days are
    # skipped, and the blocked day keeps its first-page snapshot.
    before = [r["event_id"] for r in store.events(run, "2024-01-02")]
    calls = []

    class Recording(MockCatalogSource):
        def fetch_page(self, start, end):
            calls.append((start, end))
            return super().fetch_page(start, end)

    summary = collect(store, Recording(path), run)
    assert calls == []
    assert summary.completed == summary.blocked == summary.failed == 0
    assert before == [r["event_id"] for r in store.events(run, "2024-01-02")]


def test_retry_is_noop_when_everything_done(tmp_path):
    path = write_catalog(tmp_path, [ev("A", "2024-01-01")])
    store = make_store(tmp_path)
    run = store.create_run(dt.date(2024, 1, 1), dt.date(2024, 1, 1))
    collect(store, MockCatalogSource(path), run)
    calls = []

    class Recording(MockCatalogSource):
        def fetch_page(self, start, end):
            calls.append((start, end))
            return super().fetch_page(start, end)

    collect(store, Recording(path), run)
    assert calls == []


def test_run_rejects_more_than_max_span():
    assert MAX_SPAN_DAYS == 31
    # App-level enforcement is covered through the Flask test client below.


def test_csv_parse_accepts_header_and_canonicalises(tmp_path):
    data = (
        "event_id,utc_time,magnitude\n"
        "E1,2024-02-01T05:06:07Z,3.4\n"
        "E2,2024-02-01 23:59:59,1.2\n"
    ).encode()
    parsed = parse_csv(data, "2024-02-01")
    assert [e.event_id for e in parsed.events] == ["E1", "E2"]
    assert parsed.events[0].utc_time == "2024-02-01 05:06:07"


def test_csv_rejects_other_day_duplicates_and_bad_values():
    data = (
        "event_id,utc_time,magnitude\n"
        "E1,2024-02-01T05:06:07Z,3.4\n"
        "E1,2024-02-01T06:00:00Z,2.0\n"
        "E2,2024-02-02T00:00:00Z,1.0\n"
        "E3,2024-02-01T00:00:00Z,not-a-number\n"
    ).encode()
    try:
        parse_csv(data, "2024-02-01")
    except CsvValidationError as exc:
        text = " ".join(exc.errors)
        assert "重复" in text
        assert "不属于" in text
        assert "震级无效" in text
    else:  # pragma: no cover
        raise AssertionError("expected validation failure")


def test_csv_without_header_uses_positional_columns():
    parsed = parse_csv(b"E1,2024-02-01T01:02:03Z,4.1\n", "2024-02-01")
    assert parsed.events[0].event_id == "E1"


def test_verify_includes_collected_requires_verbatim_rows():
    parsed = parse_csv(b"E1,2024-02-01T01:00:00Z,3.0\n", "2024-02-01")
    verify_includes_collected(parsed, [Event("E1", "2024-02-01 01:00:00", 3.0)])
    try:
        verify_includes_collected(parsed, [Event("E1", "2024-02-01 01:00:00", 4.0)])
    except CsvValidationError:
        pass
    else:  # pragma: no cover
        raise AssertionError("magnitude change must be rejected")


# ---------------------------------------------------------------------------
# Flask flows
# ---------------------------------------------------------------------------


def make_client(tmp_path, events, fail_dates=()):
    path = write_catalog(tmp_path, events)
    app = create_app(
        database=str(tmp_path / "web.db"),
        source=MockCatalogSource(path, fail_dates=fail_dates),
        testing=True,
    )
    return app.test_client(), app


def upload(client, run_id, day, content, filename="day.csv"):
    return client.post(
        f"/runs/{run_id}/days/{day}/upload",
        data={"csv": (io.BytesIO(content.encode("utf-8")), filename)},
        content_type="multipart/form-data",
        follow_redirects=True,
    )


def test_web_end_to_end_blocked_day_backfill_and_download(tmp_path):
    events = [ev("Q", "2024-01-01")]
    events += [ev(f"X{i:02d}", "2024-01-02", i % 24, i) for i in range(23)]
    client, _ = make_client(tmp_path, events)

    resp = client.post(
        "/runs", data={"start_date": "2024-01-01", "end_date": "2024-01-03"}
    )
    assert resp.status_code == 302
    run_id = 1

    client.post(f"/runs/{run_id}/collect")
    page = client.get(f"/runs/{run_id}").get_data(as_text=True)
    assert "待补档" in page
    assert "下载完整目录" in page

    # Download is blocked until the blocked day is backfilled.
    blocked = client.get(f"/runs/{run_id}/download")
    assert blocked.status_code == 302

    rows = "event_id,utc_time,magnitude\n"
    for i in range(23):
        rows += f"X{i:02d},2024-01-02T{i % 24:02d}:{i:02d}:00Z,3.0\n"

    resp = upload(client, run_id, "2024-01-02", rows)
    assert resp.status_code == 200
    page = resp.get_data(as_text=True)
    assert "已补档" in page
    assert "下载完整目录" in page

    out = client.get(f"/runs/{run_id}/download")
    assert out.status_code == 200
    text = out.get_data(as_text=True)
    lines = text.strip().splitlines()
    assert lines[0] == "event_id,utc_time,magnitude"
    assert len(lines) == 1 + 24  # 1 event on day 1 + 23 backfilled


def test_web_invalid_upload_preserves_progress(tmp_path):
    events = [ev(f"X{i:02d}", "2024-01-02", i % 24, i) for i in range(22)]
    client, app = make_client(tmp_path, events)
    client.post("/runs", data={"start_date": "2024-01-01", "end_date": "2024-01-02"})
    client.post("/runs/1/collect")
    store = app.extensions["store"]
    before = [dict(r) for r in store.events(1, "2024-01-02")]
    assert len(before) == 20

    # Missing an already-collected event -> rejected, progress untouched.
    bad = "event_id,utc_time,magnitude\nX00,2024-01-02T00:00:00Z,3.0\n"
    resp = upload(client, 1, "2024-01-02", bad)
    assert "校验失败" in resp.get_data(as_text=True)
    assert [dict(r) for r in store.events(1, "2024-01-02")] == before
    assert statuses(store, 1)["2024-01-02"] == STATUS_BLOCKED


def test_web_rejects_over_31_days_and_non_pending_upload(tmp_path):
    client, _ = make_client(tmp_path, [])
    resp = client.post(
        "/runs", data={"start_date": "2024-01-01", "end_date": "2024-02-01"}
    )
    assert resp.status_code == 302
    assert b"/runs/" not in resp.headers["Location"].encode()
    assert "最多 31" in client.get("/").get_data(as_text=True)

    client.post("/runs", data={"start_date": "2024-01-01", "end_date": "2024-01-01"})
    client.post("/runs/1/collect")
    resp = upload(client, 1, "2024-01-01", "E1,2024-01-01T00:00:00Z,1.0\n")
    assert "无需上传" in resp.get_data(as_text=True)


def test_web_failed_day_retry_flow(tmp_path):
    events = [ev("A", "2024-01-01")]
    path = write_catalog(tmp_path, events)
    app = create_app(
        database=str(tmp_path / "web.db"),
        source=MockCatalogSource(path, fail_dates=["2024-01-02"]),
        testing=True,
    )
    client = app.test_client()
    client.post("/runs", data={"start_date": "2024-01-01", "end_date": "2024-01-02"})
    client.post("/runs/1/collect")
    page = client.get("/runs/1").get_data(as_text=True)
    assert "待重试" in page

    app.extensions["source"] = MockCatalogSource(path)  # upstream recovered
    client.post("/runs/1/collect")
    assert statuses(app.extensions["store"], 1)["2024-01-02"] == STATUS_COMPLETE
    assert client.get("/runs/1/download").status_code == 200


def test_web_events_listing_shows_required_fields(tmp_path):
    client, _ = make_client(tmp_path, [ev("Z1", "2024-01-01", 3, 4, 2.5)])
    client.post("/runs", data={"start_date": "2024-01-01", "end_date": "2024-01-01"})
    client.post("/runs/1/collect")
    page = client.get("/runs/1").get_data(as_text=True)
    assert "Z1" in page
    assert "2024-01-01 03:04:00" in page
    assert "2.5" in page


# ---------------------------------------------------------------------------
# Network-issued manifest and partial corrections
# ---------------------------------------------------------------------------


def manifest_csv(pairs):
    """pairs: iterable of (event_id, day)."""
    rows = "event_id,day\n"
    for event_id, day in pairs:
        rows += f"{event_id},{day}\n"
    return rows


def register(client, run_id, content, filename="manifest.csv"):
    return client.post(
        f"/runs/{run_id}/manifest",
        data={"csv": (io.BytesIO(content.encode("utf-8")), filename)},
        content_type="multipart/form-data",
        follow_redirects=True,
    )


def correct(client, run_id, days, content, filename="corrected.csv"):
    return client.post(
        f"/runs/{run_id}/corrections",
        data={
            "days": list(days),
            "csv": (io.BytesIO(content.encode("utf-8")), filename),
        },
        content_type="multipart/form-data",
        follow_redirects=True,
    )


def event_rows(events):
    rows = "event_id,utc_time,magnitude\n"
    for event in events:
        rows += f"{event['id']},{event['time'][:10]} {event['time'][11:19]},{event['magnitude']}\n"
    return rows


def collected_by_day(store, run_id):
    result = {}
    for row in store.events(run_id):
        result.setdefault(row["day"], {})[row["event_id"]] = row
    return result


def test_manifest_parse_rules():
    from archive_app.csv_import import parse_manifest

    manifest = parse_manifest(manifest_csv([("A", "2024-01-02")]).encode(), "2024-01-01", "2024-01-03")
    assert manifest == {"A": "2024-01-02"}

    for bad, needle in [
        (manifest_csv([("A", "2024-01-02"), ("A", "2024-01-03")]), "重复"),
        (manifest_csv([("A", "2024-01-09")]), "不在任务"),
        (manifest_csv([("A", "not-a-day")]), "无效"),
    ]:
        try:
            parse_manifest(bad.encode(), "2024-01-01", "2024-01-03")
        except CsvValidationError as exc:
            assert any(needle in err for err in exc.errors), exc.errors
        else:  # pragma: no cover
            raise AssertionError("expected manifest rejection")


def test_manifest_diff_classifies_missing_extra_and_wrong_day(tmp_path):
    events = [
        ev("OK", "2024-01-01"),
        ev("WRONG", "2024-01-02"),  # manifest says 2024-01-03
        ev("EXTRA", "2024-01-02"),  # not in manifest
    ]
    client, app = make_client(tmp_path, events)
    client.post("/runs", data={"start_date": "2024-01-01", "end_date": "2024-01-04"})
    client.post("/runs/1/collect")

    page = register(
        client,
        1,
        manifest_csv(
            [
                ("OK", "2024-01-01"),
                ("WRONG", "2024-01-03"),
                ("MISSING", "2024-01-04"),
            ]
        ),
    ).get_data(as_text=True)

    assert "台网签发事件清单" in page
    # 2024-01-04 expects one event, is empty -> missing; 01-02 holds an unlisted
    # (EXTRA) and a wrong-day (WRONG) record.
    assert "漏项" in page and "多项" in page and "错日" in page
    assert "MISSING" in page and "EXTRA" in page
    assert "实存 2024-01-02 → 应归属 2024-01-03" in page

    store = app.extensions["store"]
    diff = store.diff(1)
    assert diff.missing == (("MISSING", "2024-01-04"),)
    assert diff.extra == (("EXTRA", "2024-01-02"),)
    assert diff.wrong_day == (("WRONG", "2024-01-02", "2024-01-03"),)
    assert diff.difference_days == {"2024-01-02", "2024-01-03", "2024-01-04"}

    # A registered manifest with differences suspends the download.
    assert client.get("/runs/1/download").status_code == 302


def test_correction_batch_moves_wrong_day_adds_missing_and_drops_extra(tmp_path):
    events = [
        ev("OK", "2024-01-01"),
        ev("WRONG", "2024-01-02"),
        ev("EXTRA", "2024-01-02"),
    ]
    client, app = make_client(tmp_path, events)
    client.post("/runs", data={"start_date": "2024-01-01", "end_date": "2024-01-04"})
    client.post("/runs/1/collect")
    register(
        client,
        1,
        manifest_csv(
            [("OK", "2024-01-01"), ("WRONG", "2024-01-03"), ("MISSING", "2024-01-04")]
        ),
    )

    store = app.extensions["store"]
    before_ok = dict(store.events(1, "2024-01-01")[0])

    # WRONG moves 02 -> 03 with a new time on 03; MISSING is added on 04; 02
    # becomes empty and is cleared by simply omitting it from the upload.  The
    # already-correct 01-01 is not selected, so it is never rewritten.
    rows = (
        "event_id,utc_time,magnitude\n"
        "WRONG,2024-01-03T05:00:00Z,3.0\n"
        "MISSING,2024-01-04T06:00:00Z,3.0\n"
    )
    resp = correct(client, 1, ["2024-01-02", "2024-01-03", "2024-01-04"], rows)
    text = resp.get_data(as_text=True)
    assert "清单与归档完全一致" in text, text

    diff = store.diff(1)
    assert not diff.has_differences
    by_day = collected_by_day(store, 1)
    assert set(by_day["2024-01-01"]) == {"OK"}
    assert "2024-01-02" not in by_day
    assert set(by_day["2024-01-03"]) == {"WRONG"}
    assert set(by_day["2024-01-04"]) == {"MISSING"}
    # An already-correct event is preserved verbatim.
    assert dict(store.events(1, "2024-01-01")[0]) == before_ok
    # Full agreement re-enables the download.
    assert client.get("/runs/1/download").status_code == 200


def test_correction_requires_wrong_day_origins_and_targets_same_batch(tmp_path):
    events = [ev("WRONG", "2024-01-02")]
    client, app = make_client(tmp_path, events)
    client.post("/runs", data={"start_date": "2024-01-01", "end_date": "2024-01-03"})
    client.post("/runs/1/collect")
    register(
        client,
        1,
        manifest_csv([("WRONG", "2024-01-03")]),
    )
    store = app.extensions["store"]
    before = [dict(r) for r in store.events(1)]

    # Selecting only the target day leaves the old row behind -> rejected, no
    # changes.
    rows = "event_id,utc_time,magnitude\nWRONG,2024-01-03T05:00:00Z,3.0\n"
    resp = correct(client, 1, ["2024-01-03"], rows)
    text = resp.get_data(as_text=True)
    assert "同一批" in text, text
    assert [dict(r) for r in store.events(1)] == before

    # Selecting both days succeeds.
    resp = correct(client, 1, ["2024-01-02", "2024-01-03"], rows)
    assert "清单与归档完全一致" in resp.get_data(as_text=True)
    assert not store.diff(1).has_differences


def test_correction_header_only_clears_selected_days(tmp_path):
    events = [ev("A", "2024-01-01"), ev("B", "2024-01-02")]
    client, app = make_client(tmp_path, events)
    client.post("/runs", data={"start_date": "2024-01-01", "end_date": "2024-01-02"})
    client.post("/runs/1/collect")
    # Manifest expects an empty 2024-01-02, so the archive's B is extra.
    register(client, 1, manifest_csv([("A", "2024-01-01")]))
    store = app.extensions["store"]
    assert store.diff(1).extra == (("B", "2024-01-02"),)

    resp = correct(client, 1, ["2024-01-02"], "event_id,utc_time,magnitude\n")
    assert "清单与归档完全一致" in resp.get_data(as_text=True)
    assert store.events(1, "2024-01-02") == []
    assert store.days(1)[1]["event_count"] == 0
    assert store.days(1)[1]["status"] == STATUS_COMPLETE


def test_correction_rejects_identifier_mismatch_and_preserves_state(tmp_path):
    events = [ev("A", "2024-01-01")]
    client, app = make_client(tmp_path, events)
    client.post("/runs", data={"start_date": "2024-01-01", "end_date": "2024-01-02"})
    client.post("/runs/1/collect")
    register(client, 1, manifest_csv([("A", "2024-01-01"), ("B", "2024-01-02")]))
    store = app.extensions["store"]
    before = [dict(r) for r in store.events(1)]

    # B is expected on 02 but the upload puts an unlisted X there instead.
    rows = "event_id,utc_time,magnitude\nA,2024-01-01T00:00:00Z,3.0\nX,2024-01-02T00:00:00Z,3.0\n"
    resp = correct(client, 1, ["2024-01-02"], rows)
    text = resp.get_data(as_text=True)
    assert "校验失败" in text and "B" in text and "X" in text, text
    assert [dict(r) for r in store.events(1)] == before
    # Download stays suspended (diff still present).
    assert client.get("/runs/1/download").status_code == 302


def test_correction_rejects_modified_already_correct_event(tmp_path):
    events = [ev("A", "2024-01-01", 1, 0, 3.0)]
    client, app = make_client(tmp_path, events)
    client.post("/runs", data={"start_date": "2024-01-01", "end_date": "2024-01-02"})
    client.post("/runs/1/collect")
    register(client, 1, manifest_csv([("A", "2024-01-01"), ("B", "2024-01-02")]))
    store = app.extensions["store"]
    before = [dict(r) for r in store.events(1)]

    rows = (
        "event_id,utc_time,magnitude\n"
        "A,2024-01-01T01:00:00Z,4.0\n"  # time and magnitude changed
        "B,2024-01-02T00:00:00Z,3.0\n"
    )
    resp = correct(client, 1, ["2024-01-01", "2024-01-02"], rows)
    text = resp.get_data(as_text=True)
    assert "校验失败" in text and "被修改" in text, text
    assert [dict(r) for r in store.events(1)] == before


def test_partial_correction_keeps_download_paused_until_all_match(tmp_path):
    events = [ev("A", "2024-01-01")]
    client, app = make_client(tmp_path, events)
    client.post("/runs", data={"start_date": "2024-01-01", "end_date": "2024-01-03"})
    client.post("/runs/1/collect")
    register(
        client,
        1,
        manifest_csv([("A", "2024-01-01"), ("B", "2024-01-02"), ("C", "2024-01-03")]),
    )
    store = app.extensions["store"]

    resp = correct(
        client, 1, ["2024-01-02"],
        "event_id,utc_time,magnitude\nB,2024-01-02T00:00:00Z,3.0\n",
    )
    assert "仍有差异" in resp.get_data(as_text=True)
    assert store.diff(1).missing == (("C", "2024-01-03"),)
    assert client.get("/runs/1/download").status_code == 302

    resp = correct(
        client, 1, ["2024-01-03"],
        "event_id,utc_time,magnitude\nC,2024-01-03T00:00:00Z,3.0\n",
    )
    assert "清单与归档完全一致" in resp.get_data(as_text=True)
    assert client.get("/runs/1/download").status_code == 200
    out = client.get("/runs/1/download").get_data(as_text=True).strip().splitlines()
    assert len(out) == 1 + 3
    assert out[0] == "event_id,utc_time,magnitude"


def test_correction_rejects_rows_outside_selected_days(tmp_path):
    events = [ev("A", "2024-01-01"), ev("B", "2024-01-02")]
    client, app = make_client(tmp_path, events)
    client.post("/runs", data={"start_date": "2024-01-01", "end_date": "2024-01-03"})
    client.post("/runs/1/collect")
    register(
        client, 1, manifest_csv([("A", "2024-01-01"), ("B", "2024-01-02"), ("C", "2024-01-03")])
    )
    store = app.extensions["store"]
    before = [dict(r) for r in store.events(1)]

    # Upload includes a row for 01-01 although only 01-03 was selected.
    rows = (
        "event_id,utc_time,magnitude\n"
        "A,2024-01-01T00:00:00Z,3.0\n"
        "C,2024-01-03T00:00:00Z,3.0\n"
    )
    resp = correct(client, 1, ["2024-01-03"], rows)
    text = resp.get_data(as_text=True)
    assert "未勾选" in text, text
    assert [dict(r) for r in store.events(1)] == before


def test_manifest_registration_is_validated_and_immutable(tmp_path):
    events = [ev("A", "2024-01-01")]
    client, app = make_client(tmp_path, events)
    client.post("/runs", data={"start_date": "2024-01-01", "end_date": "2024-01-03"})
    client.post("/runs/1/collect")
    store = app.extensions["store"]

    # Invalid manifest (day outside the run) leaves no snapshot behind.
    resp = register(client, 1, manifest_csv([("A", "2024-02-01")]))
    assert "清单校验失败" in resp.get_data(as_text=True)
    assert store.manifest(1) is None

    register(client, 1, manifest_csv([("A", "2024-01-01")]))
    assert store.manifest(1) == {"A": "2024-01-01"}
    assert not store.diff(1).has_differences

    # The manifest is fixed once registered: a second upload is refused and the
    # original snapshot is untouched.
    resp = register(client, 1, manifest_csv([("A", "2024-01-02")]))
    assert "不可替换" in resp.get_data(as_text=True)
    assert store.manifest(1) == {"A": "2024-01-01"}
    assert client.get("/runs/1/download").status_code == 200


def test_manifest_registration_requires_all_days_checked(tmp_path):
    events = [ev("A", "2024-01-01")]
    path = write_catalog(tmp_path, events)
    app = create_app(
        database=str(tmp_path / "web.db"),
        source=MockCatalogSource(path, fail_dates=["2024-01-02"]),
        testing=True,
    )
    client = app.test_client()
    client.post("/runs", data={"start_date": "2024-01-01", "end_date": "2024-01-02"})
    client.post("/runs/1/collect")
    store = app.extensions["store"]
    assert statuses(store, 1)["2024-01-02"] == STATUS_FAILED

    resp = register(client, 1, manifest_csv([("A", "2024-01-01")]))
    assert "只有全部日期查全" in resp.get_data(as_text=True)
    assert store.manifest(1) is None

    # Once the failing day is retried successfully, registration is allowed.
    app.extensions["source"] = MockCatalogSource(path)
    client.post("/runs/1/collect")
    resp = register(client, 1, manifest_csv([("A", "2024-01-01")]))
    assert "与归档完全一致" in resp.get_data(as_text=True)
    assert store.manifest(1) == {"A": "2024-01-01"}


def test_download_uses_single_snapshot(tmp_path, monkeypatch):
    events = [ev("A", "2024-01-01")]
    client, app = make_client(tmp_path, events)
    client.post("/runs", data={"start_date": "2024-01-01", "end_date": "2024-01-01"})
    client.post("/runs/1/collect")
    register(client, 1, manifest_csv([("A", "2024-01-01")]))
    store = app.extensions["store"]
    assert store.is_downloadable(1)

    # The gating verdict and the exported rows come from one transaction; if
    # the archive changes after the snapshot, the response still matches the
    # verdict it made.  Here we force the verdict to be read once.
    calls = {"n": 0}
    real = store._is_downloadable

    def counting(conn, run_id):
        calls["n"] += 1
        return real(conn, run_id)

    monkeypatch.setattr(store, "_is_downloadable", counting)
    resp = client.get("/runs/1/download")
    assert resp.status_code == 200
    assert calls["n"] == 1


def test_correction_rejected_while_days_unchecked(tmp_path):
    # A failed day means the run is not fully checked; corrections must not
    # touch a partially collected archive, and no manifest can be registered
    # yet, so the endpoint refuses before any write.
    events = [ev("A", "2024-01-01")]
    path = write_catalog(tmp_path, events)
    app = create_app(
        database=str(tmp_path / "web.db"),
        source=MockCatalogSource(path, fail_dates=["2024-01-02"]),
        testing=True,
    )
    client = app.test_client()
    client.post("/runs", data={"start_date": "2024-01-01", "end_date": "2024-01-02"})
    client.post("/runs/1/collect")
    store = app.extensions["store"]

    resp = correct(
        client, 1, ["2024-01-01"],
        "event_id,utc_time,magnitude\nA,2024-01-01T00:00:00Z,3.0\n",
    )
    assert "请先登记事件清单" in resp.get_data(as_text=True)
    assert statuses(store, 1)["2024-01-02"] == STATUS_FAILED


def test_manifest_page_lists_only_difference_days_for_selection(tmp_path):
    events = [ev("A", "2024-01-01"), ev("B", "2024-01-02")]
    client, _ = make_client(tmp_path, events)
    client.post("/runs", data={"start_date": "2024-01-01", "end_date": "2024-01-03"})
    client.post("/runs/1/collect")
    register(client, 1, manifest_csv([("A", "2024-01-01"), ("C", "2024-01-03")]))

    page = register(client, 1, manifest_csv([("A", "2024-01-01")])).get_data(as_text=True)
    # Manifest is fixed: second registration refused, existing one kept.
    assert "不可替换" in page
    page = client.get("/runs/1").get_data(as_text=True)
    checkboxes = [
        line for line in page.splitlines() if 'name="days"' in line
    ]
    # 01-01 matches; 01-02 has an extra B; 01-03 is missing C.
    assert len(checkboxes) == 2
    assert any("2024-01-02" in line for line in checkboxes)
    assert any("2024-01-03" in line for line in checkboxes)
    assert not any("2024-01-01" in line for line in checkboxes)


def test_header_only_manifest_expects_zero_everywhere(tmp_path):
    from archive_app.csv_import import parse_manifest

    empty = parse_manifest(b"event_id,day\n", "2024-01-01", "2024-01-03")
    assert empty == {}

    events = [ev("A", "2024-01-01")]
    client, app = make_client(tmp_path, events)
    client.post("/runs", data={"start_date": "2024-01-01", "end_date": "2024-01-03"})
    client.post("/runs/1/collect")
    register(client, 1, "event_id,day\n")
    store = app.extensions["store"]
    diff = store.diff(1)
    # The single archived event is now extra; every day expects zero.
    assert diff.extra == (("A", "2024-01-01"),)
    assert all(row.expected_count == 0 for row in diff.per_day)
    assert client.get("/runs/1/download").status_code == 302

    # Clearing 01-01 is enough to match a zero-expectation manifest.
    resp = correct(client, 1, ["2024-01-01"], "event_id,utc_time,magnitude\n")
    assert "清单与归档完全一致" in resp.get_data(as_text=True)
    assert client.get("/runs/1/download").status_code == 200
