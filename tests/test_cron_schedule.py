from datetime import datetime, timedelta, timezone

import pytest

from src.runtime.cron_schedule import (
    CronScheduleError,
    DEFAULT_TIMEZONE,
    advance_after_fire,
    first_fire_at,
    next_fire_at,
    parse_duration,
    parse_schedule,
    plan_due_firing,
    schedule_from_parts,
)


UTC = timezone.utc


def test_parse_at_uses_shanghai_for_a_naive_iso_datetime() -> None:
    schedule = parse_schedule("at 2026-09-09T09:30:00")

    assert schedule.kind == "at"
    assert schedule.timezone_name == DEFAULT_TIMEZONE
    assert schedule.expression == "2026-09-09T09:30:00"
    assert schedule.at_utc == datetime(2026, 9, 9, 1, 30, tzinfo=UTC)


def test_parse_at_accepts_an_aware_iso_datetime_and_z_suffix() -> None:
    offset = parse_schedule("at 2026-09-09T09:30:00+08:00")
    zulu = parse_schedule("at 2026-09-09T01:30:00Z")

    assert offset.at_utc == zulu.at_utc == datetime(
        2026, 9, 9, 1, 30, tzinfo=UTC
    )


def test_at_accepts_quoted_iso_space_and_explicit_timezone_anywhere() -> None:
    schedule = parse_schedule(
        '--tz America/New_York at "2026-01-02 09:15:00"'
    )

    assert schedule.timezone_name == "America/New_York"
    assert schedule.at_utc == datetime(2026, 1, 2, 14, 15, tzinfo=UTC)


@pytest.mark.parametrize(
    "text, message",
    [
        ("at 2026-09-09", "ISO datetime"),
        ("at not-a-date", "ISO datetime"),
        ("at 2026-09-09T09:00 --tz Mars/Olympus", "unknown IANA timezone"),
        ("at 2026-09-09T09:00 --tz", "--tz requires"),
        (
            "at 2026-09-09T09:00 --tz UTC --tz Asia/Shanghai",
            "only once",
        ),
    ],
)
def test_at_rejects_malformed_input(text: str, message: str) -> None:
    with pytest.raises(CronScheduleError, match=message):
        parse_schedule(text)


def test_at_rejects_nonexistent_dst_wall_time_and_resolves_ambiguous_early() -> None:
    with pytest.raises(CronScheduleError, match="does not exist"):
        parse_schedule(
            "at 2026-03-08T02:30:00 --tz America/New_York"
        )

    ambiguous = parse_schedule(
        "at 2026-11-01T01:30:00 --tz America/New_York"
    )
    assert ambiguous.at_utc == datetime(2026, 11, 1, 5, 30, tzinfo=UTC)


@pytest.mark.parametrize(
    "text, seconds",
    [
        ("1s", 1),
        ("30m", 1_800),
        ("2h", 7_200),
        ("1h30m", 5_400),
        ("1w2d3h4m5s", 788_645),
    ],
)
def test_parse_duration(text: str, seconds: int) -> None:
    assert parse_duration(text) == seconds


@pytest.mark.parametrize("text", ["", "0s", "1.5h", "1 hour", "m", "-1m"])
def test_parse_duration_rejects_invalid_or_zero_values(text: str) -> None:
    with pytest.raises(CronScheduleError):
        parse_duration(text)


@pytest.mark.parametrize("text", ["١h", "１h", "१h"])
def test_parse_duration_rejects_non_ascii_digits(text: str) -> None:
    with pytest.raises(CronScheduleError, match="duration must use"):
        parse_duration(text)


def test_interval_math_is_anchored_and_strictly_after_boundary() -> None:
    schedule = parse_schedule("every 15m")
    anchor = datetime(2026, 9, 8, 1, 0, tzinfo=UTC)

    assert first_fire_at(schedule, created_at=anchor) == datetime(
        2026, 9, 8, 1, 15, tzinfo=UTC
    )
    assert next_fire_at(
        schedule,
        after=datetime(2026, 9, 8, 1, 45, tzinfo=UTC),
        anchor=anchor,
    ) == datetime(2026, 9, 8, 2, 0, tzinfo=UTC)
    with pytest.raises(CronScheduleError, match="requires an anchor"):
        next_fire_at(schedule, after=anchor)


def test_one_shot_next_fire_is_strict_and_does_not_repeat() -> None:
    schedule = parse_schedule("at 2026-09-09T09:30:00Z")
    instant = datetime(2026, 9, 9, 9, 30, tzinfo=UTC)

    assert next_fire_at(
        schedule,
        after=datetime(2026, 9, 9, 9, 29, 59, tzinfo=UTC),
    ) == instant
    assert next_fire_at(schedule, after=instant) is None
    assert advance_after_fire(
        schedule,
        scheduled_for=instant,
        now=instant,
    ) is None


def test_parse_cron_requires_exactly_five_fields() -> None:
    for text in (
        "cron 0 9 * *",
        "cron 0 9 * * 1 2026",
        "cron @daily",
    ):
        with pytest.raises(CronScheduleError, match="exactly five fields"):
            parse_schedule(text)


def test_parse_cron_accepts_a_bare_five_field_expression() -> None:
    explicit = parse_schedule("cron 0 9 * * 1-5 --tz Asia/Shanghai")
    bare = parse_schedule("0 9 * * 1-5 --tz Asia/Shanghai")

    assert bare == explicit


@pytest.mark.parametrize(
    "text, message",
    [
        ("cron 60 9 * * *", "minute must be between"),
        ("cron 0 24 * * *", "hour must be between"),
        ("cron 0 0 0 * *", "day of month must be between"),
        ("cron 0 0 * FOO *", "invalid cron month"),
        ("cron */0 * * * *", "step must be greater"),
        ("cron 10-5 * * * *", "range must be ascending"),
        ("cron 1,,2 * * * *", "invalid cron minute"),
    ],
)
def test_cron_rejects_malformed_fields(text: str, message: str) -> None:
    with pytest.raises(CronScheduleError, match=message):
        parse_schedule(text)


def test_cron_next_fire_supports_ranges_steps_lists_and_names() -> None:
    schedule = parse_schedule(
        "cron 0,30 9-10/1 * JAN,MAR MON-FRI --tz Asia/Shanghai"
    )

    assert next_fire_at(
        schedule,
        after=datetime(2026, 1, 2, 1, 0, tzinfo=UTC),  # Friday, 09:00 CST
    ) == datetime(2026, 1, 2, 1, 30, tzinfo=UTC)
    assert next_fire_at(
        schedule,
        after=datetime(2026, 1, 2, 2, 30, tzinfo=UTC),
    ) == datetime(2026, 1, 5, 1, 0, tzinfo=UTC)


def test_cron_uses_vixie_or_for_restricted_month_day_and_weekday() -> None:
    # Midnight on either the first day of a month OR any Monday.
    schedule = parse_schedule("cron 0 0 1 * MON --tz UTC")

    assert next_fire_at(
        schedule,
        after=datetime(2026, 9, 1, 0, 0, tzinfo=UTC),
    ) == datetime(2026, 9, 7, 0, 0, tzinfo=UTC)
    assert next_fire_at(
        schedule,
        after=datetime(2026, 9, 28, 0, 0, tzinfo=UTC),
    ) == datetime(2026, 10, 1, 0, 0, tzinfo=UTC)


def test_cron_uses_vixie_lexical_star_semantics_for_stepped_day_field() -> None:
    # A leading star retains Vixie's star flag after expansion.  DOM */2 and
    # Monday must therefore both match; they are not ORed.  November 2 is a
    # Monday but an even day, while November 9 satisfies both fields.
    schedule = parse_schedule("cron 0 0 */2 * MON --tz UTC")

    assert next_fire_at(
        schedule,
        after=datetime(2026, 11, 1, 0, 0, tzinfo=UTC),
    ) == datetime(2026, 11, 9, 0, 0, tzinfo=UTC)


def test_cron_accepts_both_sunday_spellings() -> None:
    zero = parse_schedule("cron 0 12 * * 0 --tz UTC")
    seven = parse_schedule("cron 0 12 * * 7 --tz UTC")
    after = datetime(2026, 9, 5, 12, 0, tzinfo=UTC)  # Saturday

    assert next_fire_at(zero, after=after) == next_fire_at(seven, after=after)
    assert next_fire_at(zero, after=after) == datetime(
        2026, 9, 6, 12, 0, tzinfo=UTC
    )


def test_cron_skips_nonexistent_spring_minute() -> None:
    schedule = parse_schedule(
        "cron 30 2 * * * --tz America/New_York"
    )

    # 02:30 on March 8 does not exist. The next occurrence is March 9 at
    # 02:30 EDT (UTC-4), not a fabricated EST instant on March 8.
    assert next_fire_at(
        schedule,
        after=datetime(2026, 3, 8, 6, 0, tzinfo=UTC),
    ) == datetime(2026, 3, 9, 6, 30, tzinfo=UTC)


def test_cron_fires_repeated_fall_minute_once_without_hour_drift() -> None:
    schedule = parse_schedule(
        "cron 30 1 * * * --tz America/New_York"
    )

    first = next_fire_at(
        schedule,
        after=datetime(2026, 11, 1, 4, 0, tzinfo=UTC),
    )
    assert first == datetime(2026, 11, 1, 5, 30, tzinfo=UTC)
    # The second 01:30 wall minute is intentionally not another occurrence.
    assert next_fire_at(schedule, after=first) == datetime(
        2026, 11, 2, 6, 30, tzinfo=UTC
    )


def test_interval_is_elapsed_utc_time_across_dst() -> None:
    schedule = parse_schedule("every 24h --tz America/New_York")
    anchor = datetime(2026, 3, 7, 14, 0, tzinfo=UTC)

    assert first_fire_at(schedule, created_at=anchor) == datetime(
        2026, 3, 8, 14, 0, tzinfo=UTC
    )


def test_catch_up_plans_only_one_due_occurrence_then_skips_to_future() -> None:
    schedule = parse_schedule("every 10m")
    scheduled_for = datetime(2026, 9, 8, 1, 10, tzinfo=UTC)
    now = datetime(2026, 9, 8, 2, 5, tzinfo=UTC)

    firing = plan_due_firing(
        schedule,
        scheduled_for=scheduled_for,
        now=now,
    )

    assert firing is not None
    assert firing.scheduled_for == scheduled_for
    assert firing.is_catch_up is True
    assert firing.next_fire_at == datetime(2026, 9, 8, 2, 10, tzinfo=UTC)


def test_not_yet_due_schedule_has_no_firing_plan() -> None:
    schedule = parse_schedule("every 10m")

    assert plan_due_firing(
        schedule,
        scheduled_for=datetime(2026, 9, 8, 2, 10, tzinfo=UTC),
        now=datetime(2026, 9, 8, 2, 5, tzinfo=UTC),
    ) is None


def test_cron_catch_up_advances_strictly_past_now() -> None:
    schedule = parse_schedule("cron 0 * * * * --tz UTC")

    assert advance_after_fire(
        schedule,
        scheduled_for=datetime(2026, 9, 8, 1, 0, tzinfo=UTC),
        now=datetime(2026, 9, 8, 5, 17, tzinfo=UTC),
    ) == datetime(2026, 9, 8, 6, 0, tzinfo=UTC)


def test_schedule_can_be_reconstructed_from_durable_parts() -> None:
    original = parse_schedule("every 1h30m --tz Europe/London")
    restored = schedule_from_parts(
        original.kind,
        original.expression,
        timezone_name=original.timezone_name,
    )

    assert restored == original


def test_datetime_math_rejects_naive_runtime_instants() -> None:
    schedule = parse_schedule("cron 0 9 * * *")

    with pytest.raises(CronScheduleError, match="must include a timezone"):
        next_fire_at(schedule, after=datetime(2026, 9, 8, 1, 0))


def test_pretokenized_schedule_has_token_and_cumulative_size_bounds() -> None:
    with pytest.raises(CronScheduleError, match="too many tokens"):
        parse_schedule(["at"] * 17)
    with pytest.raises(CronScheduleError, match="too long"):
        parse_schedule(["at", "x" * 511])


def test_string_schedule_token_count_is_bounded_after_shlex() -> None:
    with pytest.raises(CronScheduleError, match="too many tokens"):
        parse_schedule(" ".join(["x"] * 17))


def test_at_boundary_conversion_raises_domain_error_not_overflow() -> None:
    with pytest.raises(CronScheduleError):
        parse_schedule("at 0001-01-01T00:00:00+14:00")
    with pytest.raises(CronScheduleError):
        parse_schedule("at 9999-12-31T23:59:59-12:00")
    with pytest.raises(CronScheduleError):
        parse_schedule("at 0001-01-01T00:00:00 --tz Asia/Shanghai")


def test_runtime_boundary_timezone_conversion_does_not_leak_overflow() -> None:
    schedule = parse_schedule("cron 0 0 * * * --tz America/New_York")
    outside_utc_range = datetime.min.replace(
        tzinfo=timezone(timedelta(hours=14))
    )

    with pytest.raises(CronScheduleError, match="supported datetime range"):
        next_fire_at(schedule, after=outside_utc_range)
    assert next_fire_at(
        schedule,
        after=datetime.min.replace(tzinfo=UTC),
    ) is None


def test_interval_advance_and_due_plan_exhaust_cleanly_at_datetime_max() -> None:
    schedule = parse_schedule("every 1s")
    boundary = datetime.max.replace(tzinfo=UTC)

    assert advance_after_fire(
        schedule,
        scheduled_for=boundary,
        now=boundary,
    ) is None
    firing = plan_due_firing(
        schedule,
        scheduled_for=boundary,
        now=boundary,
    )
    assert firing is not None
    assert firing.scheduled_for == boundary
    assert firing.next_fire_at is None
    assert firing.is_catch_up is False
