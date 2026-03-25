"""
TP4 — Pipeline Monitoring
==========================
Track execution metrics for each pipeline step (extract, transform, gold).
Generate a JSON report after each run.

Uses Python dataclasses:
    @dataclass creates a class with automatic __init__, __repr__, etc.
    Instead of writing:
        class StepMetrics:
            def __init__(self, step_name, status, ...):
                self.step_name = step_name
                self.status = status
    You just write:
        @dataclass
        class StepMetrics:
            step_name: str
            status: str = "pending"

    For mutable defaults (lists, dicts), use field(default_factory=list)
    because Python doesn't allow mutable default arguments.
"""

import json
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone


@dataclass
class StepMetrics:
    """
    Tracks metrics for a single pipeline step.

    Usage:
        step = StepMetrics(step_name="extract")
        step.status = "running"
        step.start_time = datetime.now(timezone.utc).isoformat()
        # ... run the step ...
        step.status = "success"
        step.end_time = datetime.now(timezone.utc).isoformat()
        step.rows_processed = 53188
    """
    step_name: str
    status: str = "pending"                  # pending → running → success / failed
    start_time: str = ""
    end_time: str = ""
    duration_seconds: float = 0.0
    rows_processed: int = 0
    tables_created: list = field(default_factory=list)
    errors: list = field(default_factory=list)


@dataclass
class PipelineReport:
    """
    Aggregates metrics from all pipeline steps into a single report.

    Usage:
        report = PipelineReport()
        report.add_step(step_metrics)
        report.save("pipeline_report.json")
    """
    pipeline_name: str = "KICKZ EMPIRE ELT"
    run_id: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    steps: list = field(default_factory=list)

    def add_step(self, step: StepMetrics):
        """Add a completed step's metrics to the report."""
        # TODO: Append the step to self.steps
        #pass
        self.steps.append(step)

    def to_json(self) -> str:
        """Convert the entire report to a JSON string."""
        # TODO: Use dataclasses.asdict(self) to convert to a dict,
        #       then json.dumps() with indent=2 for pretty printing
        #
        # Hint:
        #   return json.dumps(asdict(self), indent=2)
        #pass
        return json.dumps(asdict(self), indent=2)

    def save(self, filepath: str = "pipeline_report.json"):
        """Write the JSON report to a file."""
        # TODO: Call self.to_json() and write the result to filepath
        #
        # Hint:
        #   with open(filepath, "w") as f:
        #       f.write(self.to_json())
        #pass
        with open(filepath, "w") as f:
            f.write(self.to_json())
