from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest
import yaml

import chief

UTC = timezone.utc


def _write_script(path: Path, body: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")


def _base_config(job_schedule: dict, scripts: list[dict], **job_overrides: object) -> dict:
    job = {
        "name": "job-1",
        "enabled": True,
        "schedule": job_schedule,
        "scripts": scripts,
    }
    job.update(job_overrides)
    return {
        "version": 1,
        "defaults": {
            "working_dir": ".",
            "stop_on_failure": True,
            "overlap": "skip",
            "timezone": "UTC",
        },
        "jobs": [job],
    }


def _write_config(tmp_path: Path, config: dict) -> Path:
    path = tmp_path / "chief.yaml"
    path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    return path


def test_parse_and_compile_all_frequencies(tmp_path: Path) -> None:
    script = tmp_path / "scripts" / "task.py"
    _write_script(script, "print('ok')\n")
    script_item = {"path": "scripts/task.py", "timeout": 30}

    schedules = [
        {"frequency": "daily", "time": "14:30"},
        {"frequency": "weekly", "day": "monday,wednesday", "time": "09:00"},
        {"frequency": "monthly", "day_of_month": 15, "time": "08:00"},
        {"frequency": "yearly", "month": "january", "day_of_month": 1, "time": "00:00"},
        {"frequency": "interval", "every": "5m"},
        {"frequency": "custom", "minute": "0", "hour": "9", "day_of_week": "monday-friday"},
    ]

    for schedule in schedules:
        cfg = _base_config(schedule, [script_item])
        config_path = _write_config(tmp_path, cfg)
        jobs = chief.parse_config(config_path)
        runtimes = chief.compile_jobs(jobs)
        assert len(runtimes) == 1
        assert runtimes[0].compiled.kind in {"pure_cron", "hybrid", "runtime_only"}


def test_monthly_requires_day_or_ordinal_day(tmp_path: Path) -> None:
    script = tmp_path / "scripts" / "task.py"
    _write_script(script, "print('ok')\n")
    cfg = _base_config({"frequency": "monthly", "time": "09:00"}, [{"path": "scripts/task.py"}])
    config_path = _write_config(tmp_path, cfg)
    with pytest.raises(chief.ConfigError, match='monthly" requires either "day_of_month" or "ordinal \\+ day"'):
        chief.parse_config(config_path)


def test_invalid_time_rejected(tmp_path: Path) -> None:
    script = tmp_path / "scripts" / "task.py"
    _write_script(script, "print('ok')\n")
    cfg = _base_config({"frequency": "daily", "time": "25:00"}, [{"path": "scripts/task.py"}])
    config_path = _write_config(tmp_path, cfg)
    with pytest.raises(chief.ConfigError, match="HH:MM"):
        chief.parse_config(config_path)


def test_seconds_interval_rejected(tmp_path: Path) -> None:
    script = tmp_path / "scripts" / "task.py"
    _write_script(script, "print('ok')\n")
    cfg = _base_config({"frequency": "interval", "every": "30s"}, [{"path": "scripts/task.py"}])
    config_path = _write_config(tmp_path, cfg)
    with pytest.raises(chief.ConfigError, match="seconds intervals are unsupported"):
        chief.parse_config(config_path)


def test_unknown_timezone_rejected(tmp_path: Path) -> None:
    script = tmp_path / "scripts" / "task.py"
    _write_script(script, "print('ok')\n")
    cfg = _base_config(
        {"frequency": "daily", "time": "06:00", "timezone": "America/NotAZone"},
        [{"path": "scripts/task.py"}],
    )
    config_path = _write_config(tmp_path, cfg)
    with pytest.raises(chief.ConfigError, match="Invalid timezone"):
        chief.parse_config(config_path)


def test_compile_weekly_cron_expression(tmp_path: Path) -> None:
    script = tmp_path / "scripts" / "task.py"
    _write_script(script, "print('ok')\n")
    cfg = _base_config(
        {"frequency": "weekly", "day": "friday", "time": "17:30"},
        [{"path": "scripts/task.py"}],
    )
    config_path = _write_config(tmp_path, cfg)
    runtime = chief.compile_jobs(chief.parse_config(config_path))[0]
    assert runtime.compiled.kind == "pure_cron"
    assert runtime.compiled.cron_expr == "30 17 * * 5"


def test_monthly_ordinal_hybrid_guard(tmp_path: Path) -> None:
    script = tmp_path / "scripts" / "task.py"
    _write_script(script, "print('ok')\n")
    cfg = _base_config(
        {"frequency": "monthly", "ordinal": "last", "day": "friday", "time": "18:00"},
        [{"path": "scripts/task.py"}],
    )
    runtime = chief.compile_jobs(chief.parse_config(_write_config(tmp_path, cfg)))[0]
    assert runtime.compiled.kind == "hybrid"
    assert runtime.compiled.cron_expr == "0 18 * * 5"
    good = datetime(2026, 1, 30, 18, 0, tzinfo=runtime.compiled.timezone)
    bad = datetime(2026, 1, 23, 18, 0, tzinfo=runtime.compiled.timezone)
    assert runtime.compiled.guard(good) is True
    assert runtime.compiled.guard(bad) is False


def test_bounds_and_exclusions_filter_runs(tmp_path: Path) -> None:
    script = tmp_path / "scripts" / "task.py"
    _write_script(script, "print('ok')\n")
    cfg = _base_config(
        {
            "frequency": "daily",
            "time": "09:00",
            "timezone": "UTC",
            "start": "2026-01-01T00:00:00",
            "end": "2026-01-03T23:59:59",
            "exclude": ["2026-01-02"],
        },
        [{"path": "scripts/task.py"}],
    )
    runtime = chief.compile_jobs(chief.parse_config(_write_config(tmp_path, cfg)))[0]
    runs = chief.next_run_times(runtime.compiled, 5, now_utc=datetime(2025, 12, 31, 0, 0, tzinfo=UTC))
    local = [dt.astimezone(runtime.compiled.timezone).strftime("%Y-%m-%d %H:%M") for dt in runs]
    assert local == ["2026-01-01 09:00", "2026-01-03 09:00"]


def test_export_cron_labels_runtime_only(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    script = tmp_path / "scripts" / "task.py"
    _write_script(script, "print('ok')\n")
    cfg = _base_config({"frequency": "interval", "every": "90m"}, [{"path": "scripts/task.py"}])
    config_path = _write_config(tmp_path, cfg)
    exit_code = chief.command_export_cron(config_path, job_name=None)
    output = capsys.readouterr().out
    assert exit_code == 0
    assert "runtime-only schedule" in output


def test_run_stops_on_failure(tmp_path: Path) -> None:
    marker = tmp_path / "marker.txt"
    marker_literal = str(marker)
    _write_script(
        tmp_path / "scripts" / "ok_1.py",
        f"from pathlib import Path\nPath({marker_literal!r}).write_text('a', encoding='utf-8')\n",
    )
    _write_script(
        tmp_path / "scripts" / "fail.py",
        f"from pathlib import Path\np=Path({marker_literal!r})\np.write_text(p.read_text(encoding='utf-8') + 'b', encoding='utf-8')\nraise SystemExit(1)\n",
    )
    _write_script(
        tmp_path / "scripts" / "ok_2.py",
        f"from pathlib import Path\np=Path({marker_literal!r})\np.write_text(p.read_text(encoding='utf-8') + 'c', encoding='utf-8')\n",
    )

    cfg = _base_config(
        {"frequency": "daily", "time": "06:00"},
        [
            {"path": "scripts/ok_1.py"},
            {"path": "scripts/fail.py"},
            {"path": "scripts/ok_2.py"},
        ],
        stop_on_failure=True,
    )
    config_path = _write_config(tmp_path, cfg)
    assert chief.command_run(config_path, job_name="job-1", respect_schedule=False) == 1
    assert marker.read_text(encoding="utf-8") == "ab"


def test_run_respect_schedule_only_runs_when_due(tmp_path: Path) -> None:
    marker = tmp_path / "marker.txt"
    marker_literal = str(marker)
    _write_script(
        tmp_path / "scripts" / "ok.py",
        f"from pathlib import Path\nPath({marker_literal!r}).write_text('ran', encoding='utf-8')\n",
    )
    cfg = _base_config(
        {"frequency": "daily", "time": "00:00", "timezone": "UTC"},
        [{"path": "scripts/ok.py"}],
    )
    config_path = _write_config(tmp_path, cfg)

    # Unless this test runs exactly at 00:00 UTC, respect-schedule should skip.
    result = chief.command_run(config_path, job_name="job-1", respect_schedule=True)
    assert result == 0
    if datetime.now(tz=UTC).strftime("%H:%M") != "00:00":
        assert not marker.exists()


def test_no_catch_up_next_fire_strictly_in_future(tmp_path: Path) -> None:
    script = tmp_path / "scripts" / "task.py"
    _write_script(script, "print('ok')\n")
    cfg = _base_config({"frequency": "daily", "time": "06:00", "timezone": "UTC"}, [{"path": "scripts/task.py"}])
    runtime = chief.compile_jobs(chief.parse_config(_write_config(tmp_path, cfg)))[0]
    now = datetime(2026, 2, 23, 6, 0, 30, tzinfo=UTC)
    nxt = chief.next_run_after(runtime.compiled, now)
    assert nxt is not None
    assert nxt > now
    assert nxt.astimezone(runtime.compiled.timezone).strftime("%H:%M") == "06:00"


def test_overlap_modes_parse(tmp_path: Path) -> None:
    script = tmp_path / "scripts" / "task.py"
    _write_script(script, "print('ok')\n")
    cfg = {
        "version": 1,
        "defaults": {"working_dir": ".", "timezone": "UTC"},
        "jobs": [
            {
                "name": "skip-job",
                "overlap": "skip",
                "schedule": {"frequency": "daily", "time": "01:00"},
                "scripts": [{"path": "scripts/task.py"}],
            },
            {
                "name": "queue-job",
                "overlap": "queue",
                "schedule": {"frequency": "daily", "time": "02:00"},
                "scripts": [{"path": "scripts/task.py"}],
            },
            {
                "name": "parallel-job",
                "overlap": "parallel",
                "schedule": {"frequency": "daily", "time": "03:00"},
                "scripts": [{"path": "scripts/task.py"}],
            },
        ],
    }
    jobs = chief.parse_config(_write_config(tmp_path, cfg))
    overlaps = {job.name: job.overlap for job in jobs}
    assert overlaps == {"skip-job": "skip", "queue-job": "queue", "parallel-job": "parallel"}


def test_run_passes_script_args_list(tmp_path: Path) -> None:
    argv_file = tmp_path / "argv.txt"
    argv_file_literal = str(argv_file)
    _write_script(
        tmp_path / "scripts" / "capture_args.py",
        (
            "import json\n"
            "import sys\n"
            "from pathlib import Path\n"
            f"Path({argv_file_literal!r}).write_text(json.dumps(sys.argv[1:]), encoding='utf-8')\n"
        ),
    )
    cfg = _base_config(
        {"frequency": "daily", "time": "06:00"},
        [
            {
                "path": "scripts/capture_args.py",
                "args": ["--start-date", "2026-01-01", "--flag"],
            }
        ],
    )
    config_path = _write_config(tmp_path, cfg)
    assert chief.command_run(config_path, job_name="job-1", respect_schedule=False) == 0
    assert argv_file.read_text(encoding="utf-8") == '["--start-date", "2026-01-01", "--flag"]'


def test_run_passes_script_args_string(tmp_path: Path) -> None:
    argv_file = tmp_path / "argv_str.txt"
    argv_file_literal = str(argv_file)
    _write_script(
        tmp_path / "scripts" / "capture_args_str.py",
        (
            "import json\n"
            "import sys\n"
            "from pathlib import Path\n"
            f"Path({argv_file_literal!r}).write_text(json.dumps(sys.argv[1:]), encoding='utf-8')\n"
        ),
    )
    cfg = _base_config(
        {"frequency": "daily", "time": "06:00"},
        [
            {
                "path": "scripts/capture_args_str.py",
                "args": "--mode full --label \"weekly summary\"",
            }
        ],
    )
    config_path = _write_config(tmp_path, cfg)
    assert chief.command_run(config_path, job_name="job-1", respect_schedule=False) == 0
    assert argv_file.read_text(encoding="utf-8") == '["--mode", "full", "--label", "weekly summary"]'
