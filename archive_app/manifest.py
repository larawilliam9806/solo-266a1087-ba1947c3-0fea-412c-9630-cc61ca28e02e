"""Comparison between the network-issued event manifest and the archive.

The manifest is a fixed list of ``event_id -> UTC day`` pairs.  Days that do
not appear in the manifest are expected to hold zero events.  Comparing it with
the archived events yields three kinds of difference:

* **漏项** (missing)   -- the manifest lists an event that is not archived;
* **多项** (extra)     -- the archive holds an event the manifest does not list;
* **错日** (wrong day) -- the event exists, but under a different UTC day.

An archived event whose identifier and day both agree with the manifest is
considered correct and must be preserved verbatim.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping, Sequence


@dataclass(frozen=True)
class DayDiff:
    """Expected/actual identifiers for one UTC day."""

    day: str
    expected_ids: tuple[str, ...]
    actual_ids: tuple[str, ...]
    missing_ids: tuple[str, ...]
    extra_ids: tuple[str, ...]

    @property
    def expected_count(self) -> int:
        return len(self.expected_ids)

    @property
    def actual_count(self) -> int:
        return len(self.actual_ids)

    @property
    def has_difference(self) -> bool:
        return bool(self.missing_ids or self.extra_ids)


@dataclass(frozen=True)
class ManifestDiff:
    per_day: tuple[DayDiff, ...]
    # (event_id, expected_day)
    missing: tuple[tuple[str, str], ...]
    # (event_id, actual_day)
    extra: tuple[tuple[str, str], ...]
    # (event_id, actual_day, expected_day)
    wrong_day: tuple[tuple[str, str, str], ...]

    @property
    def has_differences(self) -> bool:
        return bool(self.missing or self.extra or self.wrong_day)

    @property
    def difference_days(self) -> set[str]:
        return {row.day for row in self.per_day if row.has_difference}

    @property
    def wrong_day_origins(self) -> set[str]:
        """Days from which wrong-day events must be removed."""
        return {actual_day for _eid, actual_day, _expected in self.wrong_day}

    @property
    def wrong_day_targets(self) -> set[str]:
        return {expected_day for _eid, _actual, expected_day in self.wrong_day}

    def for_day(self, day: str) -> DayDiff | None:
        for row in self.per_day:
            if row.day == day:
                return row
        return None


def build_diff(
    manifest: Mapping[str, str],
    actual: Iterable[tuple[str, str]],
    days: Sequence[str],
) -> ManifestDiff:
    """Compare ``manifest`` (id -> day) with archived ``(id, day)`` pairs.

    ``days`` is the ordered list of UTC days in the run, used so that every day
    -- including days absent from the manifest -- gets a per-day verdict.
    """
    ordered_days = list(dict.fromkeys(days))
    expected_by_day: dict[str, set[str]] = {day: set() for day in ordered_days}
    actual_by_day: dict[str, set[str]] = {day: set() for day in ordered_days}
    actual_day_of: dict[str, str] = {}

    for event_id, day in actual:
        actual_day_of[event_id] = day
        actual_by_day.setdefault(day, set()).add(event_id)
        expected_by_day.setdefault(day, set())
    for event_id, day in manifest.items():
        expected_by_day.setdefault(day, set()).add(event_id)
        actual_by_day.setdefault(day, set())

    per_day = []
    for day in expected_by_day:
        expected = expected_by_day[day]
        found = actual_by_day[day]
        per_day.append(
            DayDiff(
                day=day,
                expected_ids=tuple(sorted(expected)),
                actual_ids=tuple(sorted(found)),
                missing_ids=tuple(sorted(expected - found)),
                extra_ids=tuple(sorted(found - expected)),
            )
        )
    per_day.sort(key=lambda row: row.day)

    missing = tuple(
        sorted(
            (event_id, day)
            for event_id, day in manifest.items()
            if event_id not in actual_day_of
        )
    )
    extra = tuple(
        sorted(
            (event_id, day)
            for event_id, day in actual_day_of.items()
            if event_id not in manifest
        )
    )
    wrong_day = tuple(
        sorted(
            (event_id, actual_day_of[event_id], day)
            for event_id, day in manifest.items()
            if event_id in actual_day_of and actual_day_of[event_id] != day
        )
    )
    return ManifestDiff(
        per_day=tuple(per_day),
        missing=missing,
        extra=extra,
        wrong_day=wrong_day,
    )
