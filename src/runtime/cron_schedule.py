"""Parsing and calendar math for durable cron jobs.

The runtime persists every timestamp in UTC, but cron expressions describe
wall-clock time in an IANA timezone.  Keeping the calendar calculation in this
small dependency-free module makes those two facts explicit and, in
particular, avoids relying on cron libraries whose timezone iteration can
produce nonexistent or shifted times around daylight-saving transitions.

Supported public schedule forms are deliberately narrow::

    at 2026-09-09T09:30:00
    every 1h30m
    cron 0 9 * * 1-5
    0 9 * * 1-5

Any form may include one ``--tz <IANA name>`` option.  Its position within the
schedule is insignificant.  The default timezone is ``Asia/Shanghai``.

Cron fields use the usual five-field order (minute, hour, day of month, month,
day of week) and support ``*``, comma lists, inclusive ranges, and ``/``
steps.  English three-letter month and weekday names are accepted.  As in
Vixie cron, day-of-month and day-of-week are ORed when both are restricted.

DST policy is deterministic: a nonexistent wall-clock minute is skipped and
an ambiguous repeated wall-clock minute fires once, at its earlier occurrence.
Intervals are elapsed durations in UTC and therefore do not stretch or shrink
at a DST boundary.
"""

from __future__ import annotations

import re
import shlex
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from typing import Sequence
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


DEFAULT_TIMEZONE = "Asia/Shanghai"

_UTC = timezone.utc
_MAX_SCHEDULE_LENGTH = 512
_MAX_SCHEDULE_TOKENS = 16
_MAX_INTERVAL_SECONDS = 100 * 366 * 24 * 60 * 60
_GREGORIAN_CYCLE_DAYS = 146_097
_DURATION_PART = re.compile(r"([0-9]+)([smhdw])", flags=re.IGNORECASE)
_MONTH_NAMES = {
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}
_WEEKDAY_NAMES = {
    "sun": 0,
    "mon": 1,
    "tue": 2,
    "wed": 3,
    "thu": 4,
    "fri": 5,
    "sat": 6,
}


class CronScheduleError(ValueError):
    """Raised when a user-facing schedule is malformed or unusable."""


@dataclass(frozen=True, slots=True)
class _CronExpression:
    minutes: frozenset[int]
    hours: frozenset[int]
    days_of_month: frozenset[int]
    months: frozenset[int]
    days_of_week: frozenset[int]
    day_of_month_wildcard: bool
    day_of_week_wildcard: bool

    def matches_date(self, value: date) -> bool:
        if value.month not in self.months:
            return False
        day_of_month_matches = value.day in self.days_of_month
        # datetime.weekday() uses Monday=0; cron uses Sunday=0.
        cron_weekday = (value.weekday() + 1) % 7
        day_of_week_matches = cron_weekday in self.days_of_week
        # Vixie cron records whether either field *lexically* starts with a
        # star before expanding its values.  If so, both expanded fields must
        # match.  Thus ``*/2`` retains star semantics but is not equivalent to
        # an unrestricted ``*``.  Only two non-star fields use the familiar
        # day-of-month OR day-of-week rule.
        if self.day_of_month_wildcard or self.day_of_week_wildcard:
            return day_of_month_matches and day_of_week_matches
        return day_of_month_matches or day_of_week_matches


@dataclass(frozen=True, slots=True)
class ScheduleSpec:
    """Validated, persistence-friendly representation of one schedule.

    ``kind`` is one of ``at``, ``interval``, or ``cron``.  ``expression`` is
    the user schedule without its kind or timezone option, suitable for a
    durable column and for reconstruction through :func:`schedule_from_parts`.
    All materialized instants are aware UTC datetimes.
    """

    kind: str
    expression: str
    timezone_name: str = DEFAULT_TIMEZONE
    at_utc: datetime | None = None
    interval_seconds: int | None = None
    _cron: _CronExpression | None = field(
        default=None,
        repr=False,
        compare=False,
    )


@dataclass(frozen=True, slots=True)
class DueFiring:
    """One due occurrence and the first subsequent non-due occurrence."""

    scheduled_for: datetime
    next_fire_at: datetime | None
    is_catch_up: bool


@dataclass(frozen=True, slots=True)
class _FieldDefinition:
    label: str
    minimum: int
    maximum: int
    names: dict[str, int] | None = None
    normalize_seven_to_sunday: bool = False


_CRON_FIELDS = (
    _FieldDefinition("minute", 0, 59),
    _FieldDefinition("hour", 0, 23),
    _FieldDefinition("day of month", 1, 31),
    _FieldDefinition("month", 1, 12, _MONTH_NAMES),
    _FieldDefinition(
        "day of week",
        0,
        7,
        _WEEKDAY_NAMES,
        normalize_seven_to_sunday=True,
    ),
)


def parse_duration(value: str) -> int:
    """Return the number of seconds in a compact positive duration.

    Components may use seconds, minutes, hours, days, and weeks and must be
    adjacent, for example ``45m`` or ``1h30m``.  A duration is capped at one
    hundred years so adding it to ordinary application timestamps remains
    bounded and accidental giant integer input cannot exhaust resources.
    """

    text = str(value or "").strip().lower()
    if not text or len(text) > _MAX_SCHEDULE_LENGTH:
        raise CronScheduleError(
            "duration must use forms such as 30m, 2h, or 1h30m"
        )
    multipliers = {
        "s": 1,
        "m": 60,
        "h": 60 * 60,
        "d": 24 * 60 * 60,
        "w": 7 * 24 * 60 * 60,
    }
    total = 0
    cursor = 0
    for match in _DURATION_PART.finditer(text):
        if match.start() != cursor:
            raise CronScheduleError(
                "duration must use forms such as 30m, 2h, or 1h30m"
            )
        amount_text, unit = match.groups()
        # Limit each conversion before int() so adversarial command text does
        # not ask Python to materialize an enormous arbitrary-precision value.
        if len(amount_text) > 10:
            raise CronScheduleError("duration is too large")
        total += int(amount_text) * multipliers[unit.lower()]
        if total > _MAX_INTERVAL_SECONDS:
            raise CronScheduleError("duration is too large")
        cursor = match.end()
    if cursor != len(text):
        raise CronScheduleError(
            "duration must use forms such as 30m, 2h, or 1h30m"
        )
    if total <= 0:
        raise CronScheduleError("duration must be greater than zero")
    return total


def parse_schedule(
    value: str | Sequence[str],
    *,
    default_timezone: str = DEFAULT_TIMEZONE,
) -> ScheduleSpec:
    """Parse one complete schedule, excluding the reminder prompt.

    The command layer should split ``/cron add`` at its first whitespace-bound
    ``--`` delimiter, pass the left side here, and retain the right side as the
    prompt.  A string is tokenized with :mod:`shlex`, allowing an ISO datetime
    containing a space when quoted.  Passing an existing token sequence avoids
    a second tokenization step.
    """

    if isinstance(value, str):
        if len(value) > _MAX_SCHEDULE_LENGTH:
            raise CronScheduleError("schedule is too long")
        try:
            tokens = shlex.split(value, comments=False, posix=True)
        except ValueError as exc:
            raise CronScheduleError("schedule contains invalid quoting") from exc
        if len(tokens) > _MAX_SCHEDULE_TOKENS:
            raise CronScheduleError("schedule has too many tokens")
    else:
        tokens = []
        materialized_length = 0
        for index, token in enumerate(value):
            if index >= _MAX_SCHEDULE_TOKENS:
                raise CronScheduleError("schedule has too many tokens")
            token_text = str(token)
            materialized_length += len(token_text)
            if materialized_length > _MAX_SCHEDULE_LENGTH:
                raise CronScheduleError("schedule is too long")
            tokens.append(token_text)

    tokens, timezone_name = _extract_timezone(tokens, default_timezone)
    if not tokens:
        raise CronScheduleError("schedule kind is required: at, every, or cron")
    kind = tokens[0].casefold()
    arguments = tokens[1:]
    if kind == "at":
        if len(arguments) != 1:
            raise CronScheduleError(
                "at schedule must be: at <ISO datetime> [--tz <IANA timezone>]"
            )
        return schedule_from_parts("at", arguments[0], timezone_name=timezone_name)
    if kind == "every":
        if len(arguments) != 1:
            raise CronScheduleError(
                "interval schedule must be: every <duration> "
                "[--tz <IANA timezone>]"
            )
        return schedule_from_parts(
            "interval",
            arguments[0],
            timezone_name=timezone_name,
        )
    if kind == "cron":
        if len(arguments) != 5:
            raise CronScheduleError(
                "cron schedule requires exactly five fields: "
                "minute hour day-of-month month day-of-week"
            )
        return schedule_from_parts(
            "cron",
            " ".join(arguments),
            timezone_name=timezone_name,
        )
    # A bare five-field expression is the conventional spelling after
    # ``/cron add``.  The explicit ``cron`` prefix remains useful in APIs and
    # help text where the schedule is shown without its parent command.
    if len(tokens) == 5:
        return schedule_from_parts(
            "cron",
            " ".join(tokens),
            timezone_name=timezone_name,
        )
    raise CronScheduleError("schedule kind must be at, every, or cron")


def schedule_from_parts(
    kind: str,
    expression: str,
    *,
    timezone_name: str = DEFAULT_TIMEZONE,
) -> ScheduleSpec:
    """Reconstruct a schedule from durable kind/expression/timezone columns."""

    canonical_kind = str(kind or "").strip().casefold()
    canonical_expression = str(expression or "").strip()
    if len(canonical_expression) > _MAX_SCHEDULE_LENGTH:
        raise CronScheduleError("schedule is too long")
    zone = _load_timezone(timezone_name)
    canonical_timezone = str(getattr(zone, "key", "") or timezone_name)
    if canonical_kind == "at":
        at_utc = _parse_at_datetime(canonical_expression, zone)
        return ScheduleSpec(
            kind="at",
            expression=canonical_expression,
            timezone_name=canonical_timezone,
            at_utc=at_utc,
        )
    if canonical_kind in {"every", "interval"}:
        interval_seconds = parse_duration(canonical_expression)
        return ScheduleSpec(
            kind="interval",
            expression=canonical_expression.lower(),
            timezone_name=canonical_timezone,
            interval_seconds=interval_seconds,
        )
    if canonical_kind == "cron":
        fields = canonical_expression.split()
        if len(fields) != 5:
            raise CronScheduleError(
                "cron schedule requires exactly five fields: "
                "minute hour day-of-month month day-of-week"
            )
        compiled = _parse_cron_expression(fields)
        return ScheduleSpec(
            kind="cron",
            expression=" ".join(fields),
            timezone_name=canonical_timezone,
            _cron=compiled,
        )
    raise CronScheduleError("schedule kind must be at, interval, or cron")


def first_fire_at(spec: ScheduleSpec, *, created_at: datetime) -> datetime | None:
    """Return the first occurrence strictly after a job's creation instant."""

    return next_fire_at(spec, after=created_at, anchor=created_at)


def next_fire_at(
    spec: ScheduleSpec,
    *,
    after: datetime,
    anchor: datetime | None = None,
) -> datetime | None:
    """Return the first schedule occurrence strictly after ``after``.

    ``anchor`` is required for intervals and is normally the job's
    ``created_at`` timestamp.  It is ignored by one-shot and cron schedules.
    The result is always an aware UTC datetime.
    """

    after_utc = _aware_utc(after, label="after")
    if spec.kind == "at":
        if spec.at_utc is None:
            raise CronScheduleError("at schedule is missing its instant")
        return spec.at_utc if spec.at_utc > after_utc else None
    if spec.kind == "interval":
        if spec.interval_seconds is None or spec.interval_seconds <= 0:
            raise CronScheduleError("interval schedule is missing its duration")
        if anchor is None:
            raise CronScheduleError("interval calculation requires an anchor")
        anchor_utc = _aware_utc(anchor, label="anchor")
        interval = timedelta(seconds=spec.interval_seconds)
        if after_utc < anchor_utc:
            steps = 1
        else:
            steps = max(1, (after_utc - anchor_utc) // interval + 1)
        try:
            return anchor_utc + steps * interval
        except OverflowError:
            return None
    if spec.kind == "cron":
        return _next_cron_fire(spec, after_utc)
    raise CronScheduleError(f"unsupported schedule kind: {spec.kind}")


def advance_after_fire(
    spec: ScheduleSpec,
    *,
    scheduled_for: datetime,
    now: datetime,
) -> datetime | None:
    """Advance a fired schedule to its first occurrence strictly after now.

    A scheduler implements the documented catch-up-at-most-one policy by
    firing the persisted ``scheduled_for`` occurrence once, then calling this
    function.  All intervening missed occurrences are skipped in one step.
    """

    scheduled_utc = _aware_utc(scheduled_for, label="scheduled_for")
    now_utc = _aware_utc(now, label="now")
    if spec.kind == "at":
        return None
    if spec.kind == "interval":
        if spec.interval_seconds is None or spec.interval_seconds <= 0:
            raise CronScheduleError("interval schedule is missing its duration")
        interval = timedelta(seconds=spec.interval_seconds)
        try:
            candidate = scheduled_utc + interval
            if candidate <= now_utc:
                candidate += ((now_utc - candidate) // interval + 1) * interval
            return candidate
        except OverflowError:
            # There is no representable occurrence after datetime.max.  A
            # durable scheduler treats this exactly like an exhausted one-shot
            # and disables the job after committing its current firing.
            return None
    if spec.kind == "cron":
        return next_fire_at(spec, after=now_utc)
    raise CronScheduleError(f"unsupported schedule kind: {spec.kind}")


def plan_due_firing(
    spec: ScheduleSpec,
    *,
    scheduled_for: datetime,
    now: datetime,
) -> DueFiring | None:
    """Return one firing plan when a persisted occurrence is due.

    This helper intentionally never returns more than one occurrence, even if
    the gateway was down across many schedule boundaries.
    """

    scheduled_utc = _aware_utc(scheduled_for, label="scheduled_for")
    now_utc = _aware_utc(now, label="now")
    if scheduled_utc > now_utc:
        return None
    return DueFiring(
        scheduled_for=scheduled_utc,
        next_fire_at=advance_after_fire(
            spec,
            scheduled_for=scheduled_utc,
            now=now_utc,
        ),
        is_catch_up=scheduled_utc < now_utc,
    )


def _extract_timezone(
    tokens: Sequence[str],
    default_timezone: str,
) -> tuple[list[str], str]:
    retained: list[str] = []
    timezone_name = str(default_timezone or DEFAULT_TIMEZONE).strip()
    explicit_timezone: str | None = None
    index = 0
    while index < len(tokens):
        token = str(tokens[index])
        if token.casefold() != "--tz":
            retained.append(token)
            index += 1
            continue
        if explicit_timezone is not None:
            raise CronScheduleError("timezone may be specified only once")
        if index + 1 >= len(tokens) or not str(tokens[index + 1]).strip():
            raise CronScheduleError("--tz requires an IANA timezone name")
        explicit_timezone = str(tokens[index + 1]).strip()
        index += 2
    if explicit_timezone is not None:
        timezone_name = explicit_timezone
    zone = _load_timezone(timezone_name)
    return retained, str(getattr(zone, "key", "") or timezone_name)


def _load_timezone(value: str) -> ZoneInfo:
    name = str(value or "").strip()
    if not name:
        raise CronScheduleError("timezone name cannot be empty")
    try:
        return ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise CronScheduleError(f"unknown IANA timezone: {name}") from exc


def _parse_at_datetime(value: str, zone: ZoneInfo) -> datetime:
    text = str(value or "").strip()
    if not text or not re.search(r"[Tt ]", text):
        raise CronScheduleError(
            "at schedule requires an ISO datetime, for example "
            "2026-09-09T09:30:00"
        )
    normalized = text[:-1] + "+00:00" if text[-1:].casefold() == "z" else text
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise CronScheduleError(f"invalid ISO datetime: {text}") from exc
    if parsed.tzinfo is None:
        candidates = _valid_local_candidates(parsed, zone)
        if not candidates:
            raise CronScheduleError(
                f"local datetime does not exist in {zone.key}: {text}"
            )
        # A repeated local time names two instants.  Choose the earlier one so
        # a one-shot remains deterministic without silently firing twice.
        parsed = min(candidates, key=lambda candidate: candidate.astimezone(_UTC))
    try:
        return parsed.astimezone(_UTC)
    except OverflowError as exc:
        raise CronScheduleError("ISO datetime is outside the supported range") from exc


def _parse_cron_expression(fields: Sequence[str]) -> _CronExpression:
    parsed: list[frozenset[int]] = []
    wildcards: list[bool] = []
    for value, definition in zip(fields, _CRON_FIELDS):
        values, wildcard = _parse_cron_field(value, definition)
        parsed.append(values)
        wildcards.append(wildcard)
    return _CronExpression(
        minutes=parsed[0],
        hours=parsed[1],
        days_of_month=parsed[2],
        months=parsed[3],
        days_of_week=parsed[4],
        day_of_month_wildcard=wildcards[2],
        day_of_week_wildcard=wildcards[4],
    )


def _parse_cron_field(
    value: str,
    definition: _FieldDefinition,
) -> tuple[frozenset[int], bool]:
    text = str(value or "").strip().casefold()
    if not text:
        raise CronScheduleError(f"cron {definition.label} field is empty")
    selected: set[int] = set()
    for component in text.split(","):
        if not component:
            raise CronScheduleError(
                f"invalid cron {definition.label} field: {value}"
            )
        if component.count("/") > 1:
            raise CronScheduleError(
                f"invalid cron {definition.label} step: {component}"
            )
        base, separator, step_text = component.partition("/")
        if separator:
            if not step_text.isascii() or not step_text.isdigit():
                raise CronScheduleError(
                    f"invalid cron {definition.label} step: {component}"
                )
            step = int(step_text)
            if step <= 0:
                raise CronScheduleError(
                    f"cron {definition.label} step must be greater than zero"
                )
        else:
            step = 1

        if base == "*":
            start, end = definition.minimum, definition.maximum
        elif base.count("-") == 1:
            start_text, end_text = base.split("-", 1)
            start = _parse_cron_atom(start_text, definition)
            end = _parse_cron_atom(end_text, definition)
            if start > end:
                raise CronScheduleError(
                    f"cron {definition.label} range must be ascending: {base}"
                )
        elif "-" in base or not base:
            raise CronScheduleError(
                f"invalid cron {definition.label} range: {base}"
            )
        else:
            start = _parse_cron_atom(base, definition)
            end = definition.maximum if separator else start

        for item in range(start, end + 1, step):
            if definition.normalize_seven_to_sunday and item == 7:
                item = 0
            selected.add(item)
    if not selected:
        raise CronScheduleError(f"cron {definition.label} selects no values")
    return frozenset(selected), text.startswith("*")


def _parse_cron_atom(value: str, definition: _FieldDefinition) -> int:
    text = str(value or "").strip().casefold()
    if definition.names is not None and text in definition.names:
        result = definition.names[text]
    elif text.isascii() and text.isdigit():
        result = int(text)
    else:
        raise CronScheduleError(
            f"invalid cron {definition.label} value: {value or '?'}"
        )
    if result < definition.minimum or result > definition.maximum:
        raise CronScheduleError(
            f"cron {definition.label} must be between "
            f"{definition.minimum} and {definition.maximum}"
        )
    return result


def _aware_utc(value: datetime, *, label: str) -> datetime:
    if not isinstance(value, datetime):
        raise CronScheduleError(f"{label} must be a datetime")
    try:
        offset = value.utcoffset()
    except OverflowError as exc:
        raise CronScheduleError(
            f"{label} is outside the supported datetime range"
        ) from exc
    if value.tzinfo is None or offset is None:
        raise CronScheduleError(f"{label} must include a timezone")
    try:
        return value.astimezone(_UTC)
    except OverflowError as exc:
        raise CronScheduleError(
            f"{label} is outside the supported datetime range"
        ) from exc


def _valid_local_candidates(value: datetime, zone: ZoneInfo) -> tuple[datetime, ...]:
    naive = value.replace(tzinfo=None)
    candidates: dict[datetime, datetime] = {}
    for fold in (0, 1):
        local = naive.replace(tzinfo=zone, fold=fold)
        try:
            instant = local.astimezone(_UTC)
            round_trip = instant.astimezone(zone)
        except OverflowError:
            continue
        if (
            round_trip.replace(tzinfo=None) == naive
            and round_trip.fold == fold
        ):
            candidates[instant] = local
    return tuple(candidates[instant] for instant in sorted(candidates))


def _next_cron_fire(spec: ScheduleSpec, after_utc: datetime) -> datetime | None:
    compiled = spec._cron
    if compiled is None:
        compiled = _parse_cron_expression(spec.expression.split())
    zone = _load_timezone(spec.timezone_name)
    try:
        local_after = after_utc.astimezone(zone)
    except OverflowError:
        return None
    candidate_date = local_after.date()
    for _ in range(_GREGORIAN_CYCLE_DAYS + 1):
        if compiled.matches_date(candidate_date):
            for hour in sorted(compiled.hours):
                for minute in sorted(compiled.minutes):
                    wall_time = datetime.combine(
                        candidate_date,
                        time(hour=hour, minute=minute),
                    )
                    candidates = _valid_local_candidates(wall_time, zone)
                    if not candidates:
                        # Spring-forward gaps do not name an actual instant.
                        continue
                    # Fall-back repeats one wall minute.  Select only its
                    # earlier instant so a daily job cannot run twice.
                    try:
                        instant = min(
                            candidate.astimezone(_UTC) for candidate in candidates
                        )
                    except OverflowError:
                        continue
                    if instant > after_utc:
                        return instant
        try:
            candidate_date += timedelta(days=1)
        except OverflowError:
            return None
    # The Gregorian date/weekday pattern repeats after 400 years.  Reaching
    # this point means the otherwise syntactically valid expression can never
    # select a real date (for example, day 31 restricted to February).
    return None


__all__ = [
    "CronScheduleError",
    "DEFAULT_TIMEZONE",
    "DueFiring",
    "ScheduleSpec",
    "advance_after_fire",
    "first_fire_at",
    "next_fire_at",
    "parse_duration",
    "parse_schedule",
    "plan_due_firing",
    "schedule_from_parts",
]
