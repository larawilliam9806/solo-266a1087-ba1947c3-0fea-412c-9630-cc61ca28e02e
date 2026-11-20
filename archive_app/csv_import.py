"""Validation for full-day CSV backfills and manifest-driven corrections."""

from __future__ import annotations

import csv
import io
import math
from dataclasses import dataclass
from typing import Mapping, Sequence

from .catalog import Event, canonical_time, parse_day
from .manifest import ManifestDiff


ID_NAMES = {"event_id", "eventid", "id", "编号", "事件编号"}
TIME_NAMES = {"utc_time", "time", "utc", "datetime", "date_time", "发震时刻"}
MAG_NAMES = {"magnitude", "mag", "m", "震级"}
DAY_NAMES = {
    "day",
    "utc_day",
    "utc_date",
    "date",
    "日期",
    "utc日期",
    "归属日",
    "应归属日",
    "应归属utc日",
}


class CsvValidationError(ValueError):
    def __init__(self, errors: list[str]):
        super().__init__("; ".join(errors))
        self.errors = errors


@dataclass
class ParsedCsv:
    events: list[Event]


def _decode(data: bytes) -> str:
    for encoding in ("utf-8-sig", "utf-8", "gb18030"):
        try:
            return data.decode(encoding)
        except UnicodeDecodeError:
            continue
    raise CsvValidationError(["文件编码无法识别，请使用 UTF-8"])


def _column_map(header: list[str]) -> dict[str, int] | None:
    names = [cell.strip().lower() for cell in header]
    mapping: dict[str, int] = {}
    for key, aliases in (("id", ID_NAMES), ("time", TIME_NAMES), ("mag", MAG_NAMES)):
        for index, name in enumerate(names):
            if name in aliases:
                mapping[key] = index
                break
    if len(mapping) != 3:
        return None
    return mapping


def _looks_like_header(row: list[str]) -> bool:
    if len(row) < 3:
        return False
    try:
        canonical_time(row[1])
        float(row[2])
    except ValueError:
        return True
    return False


def parse_csv(data: bytes, day: str) -> ParsedCsv:
    """Parse and validate a day CSV, returning canonical events.

    Rules:
      * every record's UTC issuance time falls on ``day``;
      * event identifiers are unique within the file;
      * column order is id, utc_time, magnitude unless a header names them.
    """
    text = _decode(data)
    reader = csv.reader(io.StringIO(text))
    rows = [row for row in reader if any(cell.strip() for cell in row)]
    if not rows:
        raise CsvValidationError(["CSV 文件为空"])

    mapping = None
    if _looks_like_header(rows[0]):
        mapping = _column_map(rows[0])
        if mapping is None:
            raise CsvValidationError(
                ["表头无法识别，应包含事件编号、UTC 发震时刻、震级三列"]
            )
        rows = rows[1:]
        if not rows:
            raise CsvValidationError(["CSV 文件没有数据行"])
    else:
        mapping = {"id": 0, "time": 1, "mag": 2}

    events: list[Event] = []
    errors: list[str] = []
    seen: set[str] = set()
    for line_no, row in enumerate(rows, start=2 if mapping else 1):
        if len(row) < 3:
            errors.append(f"第 {line_no} 行缺少列")
            continue
        event_id = row[mapping["id"]].strip()
        time_text = row[mapping["time"]].strip()
        mag_text = row[mapping["mag"]].strip()
        if not event_id:
            errors.append(f"第 {line_no} 行事件编号为空")
            continue
        if event_id in seen:
            errors.append(f"第 {line_no} 行事件编号重复: {event_id}")
            continue
        try:
            utc_time = canonical_time(time_text)
        except ValueError:
            errors.append(f"第 {line_no} 行 UTC 发震时刻无效: {time_text!r}")
            continue
        try:
            magnitude = float(mag_text)
            if not math.isfinite(magnitude):
                raise ValueError
        except ValueError:
            errors.append(f"第 {line_no} 行震级无效: {mag_text!r}")
            continue
        if utc_time[:10] != day:
            errors.append(
                f"第 {line_no} 行不属于 {day}（UTC {utc_time}），文件只能包含当天事件"
            )
            continue
        seen.add(event_id)
        events.append(Event(event_id=event_id, utc_time=utc_time, magnitude=magnitude))

    if errors:
        raise CsvValidationError(errors)
    return ParsedCsv(events=events)


def verify_includes_collected(parsed: ParsedCsv, existing: list[Event]) -> None:
    """Every already-collected record must appear verbatim in the upload."""
    by_id = {event.event_id: event for event in parsed.events}
    missing: list[str] = []
    changed: list[str] = []
    for old in existing:
        new = by_id.get(old.event_id)
        if new is None:
            missing.append(old.event_id)
        elif new.utc_time != old.utc_time or not _same_magnitude(new.magnitude, old.magnitude):
            changed.append(old.event_id)
    errors: list[str] = []
    if missing:
        errors.append("已采记录未原样列入: " + ", ".join(sorted(missing)))
    if changed:
        errors.append("已采记录内容被修改: " + ", ".join(sorted(changed)))
    if errors:
        raise CsvValidationError(errors)


def _same_magnitude(a: float, b: float) -> bool:
    return math.isclose(a, b, rel_tol=0.0, abs_tol=1e-9)


# ---------------------------------------------------------------------------
# Network manifest
# ---------------------------------------------------------------------------


def parse_manifest(
    data: bytes,
    start: str,
    end: str,
) -> dict[str, str]:
    """Parse the network-issued manifest into ``event_id -> UTC day``.

    The manifest is fixed once registered: each event ID is unique across the
    file and every day must fall inside the run's UTC range.  Days absent from
    the manifest are taken to expect zero events.
    """
    text = _decode(data)
    reader = csv.reader(io.StringIO(text))
    rows = [row for row in reader if any(cell.strip() for cell in row)]
    if not rows:
        raise CsvValidationError(["清单文件为空"])

    mapping = None
    has_header = False
    if _looks_like_manifest_header(rows[0]):
        mapping = _manifest_column_map(rows[0])
        if mapping is None:
            raise CsvValidationError(
                ["表头无法识别，应包含事件编号与应归属 UTC 日两列"]
            )
        has_header = True
        rows = rows[1:]
    else:
        mapping = {"id": 0, "day": 1}

    first = parse_day(start)
    last = parse_day(end)
    manifest: dict[str, str] = {}
    errors: list[str] = []
    for line_no, row in enumerate(rows, start=2 if has_header else 1):
        if len(row) <= max(mapping["id"], mapping["day"]):
            errors.append(f"第 {line_no} 行缺少列")
            continue
        event_id = row[mapping["id"]].strip()
        day_text = row[mapping["day"]].strip()
        if not event_id:
            errors.append(f"第 {line_no} 行事件编号为空")
            continue
        if event_id in manifest:
            errors.append(f"第 {line_no} 行事件编号重复: {event_id}")
            continue
        try:
            day = parse_day(day_text)
        except ValueError:
            errors.append(f"第 {line_no} 行应归属日无效: {day_text!r}")
            continue
        if not (first <= day <= last):
            errors.append(
                f"第 {line_no} 行应归属日 {day.isoformat()} 不在任务 "
                f"{start} ~ {end} 内"
            )
            continue
        manifest[event_id] = day.isoformat()

    if errors:
        raise CsvValidationError(errors)
    return manifest


def _looks_like_manifest_header(row: list[str]) -> bool:
    if len(row) < 2:
        return False
    try:
        parse_day(row[1])
    except ValueError:
        return True
    return False


def _manifest_column_map(header: list[str]) -> dict[str, int] | None:
    names = [cell.strip().lower() for cell in header]
    mapping: dict[str, int] = {}
    for key, aliases in (("id", ID_NAMES), ("day", DAY_NAMES)):
        for index, name in enumerate(names):
            if name in aliases:
                mapping[key] = index
                break
    if len(mapping) != 2:
        return None
    return mapping


# ---------------------------------------------------------------------------
# Correction upload for selected days
# ---------------------------------------------------------------------------


def parse_correction_csv(
    data: bytes,
    selected_days: Sequence[str],
    manifest: Mapping[str, str],
    collected: Mapping[str, Sequence[Event]],
) -> dict[str, list[Event]]:
    """Parse a full replacement CSV covering exactly ``selected_days``.

    ``collected`` maps every already-archived day to its events.  Rules:

    * rows keep the original three columns (id, UTC time, magnitude);
    * a row's UTC date must be the day the manifest assigns its identifier --
      this is how a wrong-day event is corrected: it is re-listed with a time
      on its expected day;
    * each selected day's identifiers must equal the manifest exactly;
    * a file may contain only selected days; a header-only file (or a blank
      one) clears every selected day, which must therefore expect zero events;
    * archived events whose identifier *and* day already match the manifest are
      preserved verbatim (time and magnitude must not change).
    """
    selected = list(dict.fromkeys(selected_days))
    if not selected:
        raise CsvValidationError(["请先勾选要修正的日期"])
    selected_set = set(selected)

    expected_ids_by_day: dict[str, set[str]] = {day: set() for day in selected}
    for eid, expected_day in manifest.items():
        if expected_day in selected_set:
            expected_ids_by_day[expected_day].add(eid)

    text = _decode(data)
    reader = csv.reader(io.StringIO(text))
    rows = [row for row in reader if any(cell.strip() for cell in row)]

    mapping: dict[str, int] = {"id": 0, "time": 1, "mag": 2}
    if rows and _looks_like_header(rows[0]):
        found = _column_map(rows[0])
        if found is None:
            raise CsvValidationError(
                ["表头无法识别，应包含事件编号、UTC 发震时刻、震级三列"]
            )
        mapping = found
        rows = rows[1:]

    parsed: dict[str, list[Event]] = {day: [] for day in selected}
    errors: list[str] = []
    seen: dict[str, int] = {}
    for line_no, row in enumerate(rows, start=2):
        if len(row) < 3:
            errors.append(f"第 {line_no} 行缺少列")
            continue
        event_id = row[mapping["id"]].strip()
        time_text = row[mapping["time"]].strip()
        mag_text = row[mapping["mag"]].strip()
        if not event_id:
            errors.append(f"第 {line_no} 行事件编号为空")
            continue
        if event_id in seen:
            errors.append(f"第 {line_no} 行事件编号重复: {event_id}")
            continue
        try:
            utc_time = canonical_time(time_text)
        except ValueError:
            errors.append(f"第 {line_no} 行 UTC 发震时刻无效: {time_text!r}")
            continue
        try:
            magnitude = float(mag_text)
            if not math.isfinite(magnitude):
                raise ValueError
        except ValueError:
            errors.append(f"第 {line_no} 行震级无效: {mag_text!r}")
            continue

        day = manifest.get(event_id)
        if day is None:
            errors.append(f"第 {line_no} 行事件不在清单中: {event_id}")
            continue
        if day not in selected_set:
            errors.append(f"第 {line_no} 行应归属 {day}，但该日未勾选")
            continue
        if utc_time[:10] != day:
            errors.append(
                f"第 {line_no} 行 UTC 时刻 {utc_time} 不属于应归属日 {day}"
            )
            continue
        seen[event_id] = line_no
        parsed[day].append(Event(event_id=event_id, utc_time=utc_time, magnitude=magnitude))

    if errors:
        raise CsvValidationError(errors)

    # Every selected day's identifier set must equal the manifest exactly.
    for day in selected:
        expected = expected_ids_by_day[day]
        loaded = {e.event_id for e in parsed[day]}
        if missing := sorted(expected - loaded):
            errors.append(f"{day} 缺少清单事件: " + ", ".join(missing))
        if extra := sorted(loaded - expected):
            errors.append(f"{day} 出现清单外事件: " + ", ".join(extra))
    if errors:
        raise CsvValidationError(errors)

    # Already-correct archived events must be preserved verbatim.
    for day in selected:
        new_by_id = {e.event_id: e for e in parsed[day]}
        for old in collected.get(day, ()):
            if manifest.get(old.event_id) != day:
                continue
            new = new_by_id[old.event_id]
            if new.utc_time != old.utc_time or not _same_magnitude(
                new.magnitude, old.magnitude
            ):
                errors.append(f"{day} 已正确归档的事件被修改: {old.event_id}")
    if errors:
        raise CsvValidationError(errors)
    return parsed


def verify_correction_batch(
    selected_days: Sequence[str],
    diff: ManifestDiff,
) -> None:
    """Cross-day checks that per-day parsing cannot see.

    A wrong-day event must leave its original day and enter its expected day in
    the same batch, so both days have to be selected together.  Selecting just
    one side would either duplicate the identifier or leave the manifest short
    one event.
    """
    selected = set(selected_days)
    errors: list[str] = []
    for event_id, actual_day, expected_day in diff.wrong_day:
        here = actual_day in selected
        there = expected_day in selected
        if here == there:
            continue
        missing_day = expected_day if here else actual_day
        errors.append(
            f"错日事件 {event_id} 须在同一批从原日 {actual_day} 移出、"
            f"补入应归属日 {expected_day}，请同时勾选 {missing_day}"
        )
    if errors:
        raise CsvValidationError(errors)
