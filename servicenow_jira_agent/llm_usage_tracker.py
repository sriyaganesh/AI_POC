"""
Tracks every LLM (Ollama) call made while classifying incidents -- when it
happened, which model, how many tokens it used, how long it took, and
whether it succeeded -- and provides a summary of that usage.

Each call is appended as one JSON line to a local log file (default:
llm_usage_log.jsonl next to this script), so usage accumulates across runs
and can be summarized at any time, including from a separate process while
--watch is still running.

Usage from code (see incident_monitor_ollama.py for the real integration):

    from .llm_usage_tracker import LLMUsageTracker

    tracker = LLMUsageTracker()
    with tracker.track(model="qwen3:4b", purpose="classify_incident") as call:
        response = llm.invoke([...])
        call.set_response(response)   # pulls token counts off response.usage_metadata
        call.result = "Jira"          # optional: whatever the call decided

    tracker.print_summary()

Run directly to print a summary of everything logged so far:

    python -m servicenow_jira_agent.llm_usage_tracker
    python -m servicenow_jira_agent.llm_usage_tracker --log path/to/log.jsonl
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections import Counter
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

DEFAULT_LOG_PATH = Path(__file__).resolve().parent / "llm_usage_log.jsonl"


@dataclass
class LLMCallRecord:
    """Everything recorded about a single LLM call."""

    timestamp: str
    model: str
    purpose: str
    success: bool
    duration_ms: float
    input_tokens: int | None = None
    output_tokens: int | None = None
    total_tokens: int | None = None
    result: str | None = None
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "model": self.model,
            "purpose": self.purpose,
            "success": self.success,
            "duration_ms": round(self.duration_ms, 1),
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "total_tokens": self.total_tokens,
            "result": self.result,
            "error": self.error,
        }


@dataclass
class _InFlightCall:
    """Handle yielded by LLMUsageTracker.track() for the duration of one call."""

    model: str
    purpose: str
    _start: float = field(default_factory=time.perf_counter)
    input_tokens: int | None = None
    output_tokens: int | None = None
    total_tokens: int | None = None
    result: str | None = None

    def set_response(self, response: Any) -> None:
        """
        Pull token usage off a langchain ChatOllama response, if present.
        Safe to call with any object -- missing/unexpected shapes just leave
        the token counts as None rather than raising.
        """
        usage = getattr(response, "usage_metadata", None)
        if usage:
            self.input_tokens = usage.get("input_tokens")
            self.output_tokens = usage.get("output_tokens")
            self.total_tokens = usage.get("total_tokens")
        content = getattr(response, "content", None)
        if content and self.result is None:
            self.result = str(content).strip()[:200]

    def elapsed_ms(self) -> float:
        return (time.perf_counter() - self._start) * 1000


class LLMUsageTracker:
    """
    Records LLM call metadata (tokens, latency, success/failure) both
    in-memory (for this process's session summary) and to a JSONL log file
    (so usage accumulates across runs / processes).
    """

    def __init__(self, log_path: Path | str = DEFAULT_LOG_PATH) -> None:
        self.log_path = Path(log_path)
        self.records: list[LLMCallRecord] = []

    @contextmanager
    def track(self, model: str, purpose: str = "llm_call") -> Iterator[_InFlightCall]:
        """
        Context manager wrapping a single LLM call:

            with tracker.track(model="qwen3:4b", purpose="classify_incident") as call:
                response = llm.invoke(...)
                call.set_response(response)

        Records a success entry (with whatever token/result info was set)
        when the block exits normally, or a failure entry (with the
        exception message) if it raises -- the exception is always
        re-raised, tracking never swallows errors.
        """
        call = _InFlightCall(model=model, purpose=purpose)
        try:
            yield call
        except Exception as e:  # noqa: BLE001 - record then re-raise
            self._append(
                LLMCallRecord(
                    timestamp=datetime.now(timezone.utc).isoformat(),
                    model=model,
                    purpose=purpose,
                    success=False,
                    duration_ms=call.elapsed_ms(),
                    input_tokens=call.input_tokens,
                    output_tokens=call.output_tokens,
                    total_tokens=call.total_tokens,
                    result=call.result,
                    error=str(e),
                )
            )
            raise
        else:
            self._append(
                LLMCallRecord(
                    timestamp=datetime.now(timezone.utc).isoformat(),
                    model=model,
                    purpose=purpose,
                    success=True,
                    duration_ms=call.elapsed_ms(),
                    input_tokens=call.input_tokens,
                    output_tokens=call.output_tokens,
                    total_tokens=call.total_tokens,
                    result=call.result,
                )
            )

    def _append(self, record: LLMCallRecord) -> None:
        self.records.append(record)
        try:
            with self.log_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(record.to_dict()) + "\n")
        except OSError as e:
            print(f"Warning: could not write LLM usage log ({e})", file=sys.stderr)

    def session_summary(self) -> dict[str, Any]:
        """Summarize only the calls made by this LLMUsageTracker instance."""
        return summarize(self.records)

    def print_summary(self, title: str = "LLM usage summary (this session)") -> None:
        print_summary(self.session_summary(), title=title)


def summarize(records: list[LLMCallRecord]) -> dict[str, Any]:
    """Aggregate a list of LLMCallRecord into totals/averages/breakdowns."""
    total_calls = len(records)
    ok = [r for r in records if r.success]
    failed = [r for r in records if not r.success]

    input_tokens = [r.input_tokens for r in records if r.input_tokens is not None]
    output_tokens = [r.output_tokens for r in records if r.output_tokens is not None]
    total_tokens = [r.total_tokens for r in records if r.total_tokens is not None]
    durations = [r.duration_ms for r in records]

    return {
        "total_calls": total_calls,
        "successful_calls": len(ok),
        "failed_calls": len(failed),
        "total_input_tokens": sum(input_tokens),
        "total_output_tokens": sum(output_tokens),
        "total_tokens": sum(total_tokens),
        "avg_input_tokens": round(sum(input_tokens) / len(input_tokens), 1) if input_tokens else 0,
        "avg_output_tokens": round(sum(output_tokens) / len(output_tokens), 1) if output_tokens else 0,
        "avg_duration_ms": round(sum(durations) / len(durations), 1) if durations else 0,
        "min_duration_ms": round(min(durations), 1) if durations else 0,
        "max_duration_ms": round(max(durations), 1) if durations else 0,
        "calls_by_model": dict(Counter(r.model for r in records)),
        "calls_by_purpose": dict(Counter(r.purpose for r in records)),
        "calls_by_result": dict(Counter(r.result for r in ok if r.result)),
        "first_call_at": records[0].timestamp if records else None,
        "last_call_at": records[-1].timestamp if records else None,
    }


def print_summary(summary: dict[str, Any], title: str = "LLM usage summary") -> None:
    print(f"\n=== {title} ===")
    if summary["total_calls"] == 0:
        print("No LLM calls recorded.")
        return

    print(f"Calls:            {summary['total_calls']} "
          f"({summary['successful_calls']} ok, {summary['failed_calls']} failed)")
    print(f"Tokens used:      {summary['total_tokens']} total "
          f"({summary['total_input_tokens']} input + {summary['total_output_tokens']} output)")
    print(f"Avg tokens/call:  {summary['avg_input_tokens']} input, {summary['avg_output_tokens']} output")
    print(f"Latency:          avg {summary['avg_duration_ms']}ms, "
          f"min {summary['min_duration_ms']}ms, max {summary['max_duration_ms']}ms")
    print(f"By model:         {summary['calls_by_model']}")
    print(f"By purpose:       {summary['calls_by_purpose']}")
    print(f"By result:        {summary['calls_by_result']}")
    print(f"First call:       {summary['first_call_at']}")
    print(f"Last call:        {summary['last_call_at']}")
    print()


def load_records(log_path: Path) -> list[LLMCallRecord]:
    if not log_path.exists():
        return []
    records: list[LLMCallRecord] = []
    for line in log_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            data = json.loads(line)
            records.append(LLMCallRecord(**data))
        except (json.JSONDecodeError, TypeError):
            continue
    return records


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Print a summary of logged LLM (Ollama) calls: how many "
        "calls were made, how many tokens were used, latency, and outcomes."
    )
    parser.add_argument(
        "--log",
        type=Path,
        default=DEFAULT_LOG_PATH,
        help=f"Path to the LLM usage JSONL log file (default: {DEFAULT_LOG_PATH})",
    )
    args = parser.parse_args()

    records = load_records(args.log)
    print_summary(summarize(records), title=f"LLM usage summary ({args.log})")


if __name__ == "__main__":
    main()
