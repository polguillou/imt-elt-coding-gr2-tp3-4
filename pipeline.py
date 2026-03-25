"""
KICKZ EMPIRE — ELT Pipeline Orchestrator
==========================================
Main entry point for the ELT pipeline.

Executes sequentially:
    1. Extract  → Load CSV files into Bronze (raw, as-is)
    2. Transform → Clean Bronze → Silver (conformed)
    3. Gold      → Aggregate Silver → Gold (business-ready)

Usage:
    python pipeline.py                  # Run the full pipeline
    python pipeline.py --step extract   # Run extraction only
    python pipeline.py --step transform # Run transformation only
    python pipeline.py --step gold      # Run Gold layer only
"""

import argparse
import time
from datetime import datetime, timezone

from src.extract import extract_all
from src.transform import transform_all
from src.gold import create_gold_layer


# TODO (TP4): Import the monitoring classes
#   from src.monitoring import PipelineReport, StepMetrics
from src.monitoring import PipelineReport, StepMetrics


def run_pipeline(step: str = "all"):
    """
    Run the ELT pipeline.

    Args:
        step (str): Which step to execute.
            - "all"       : full pipeline
            - "extract"   : extraction only (Bronze)
            - "transform" : transformation only (Silver)
            - "gold"      : Gold layer only
    """
    print("=" * 60)
    print("  🏪 KICKZ EMPIRE — ELT Pipeline")
    print("=" * 60)

    # TODO (TP4): Create a PipelineReport instance to track execution
    #   report = PipelineReport()
    report = PipelineReport()
    t0 = time.time()

    if step in ("all", "extract"):
        # TODO (TP4): Track the extract step with StepMetrics
        #   step_metrics = StepMetrics(step_name="extract")
        #   step_metrics.status = "running"
        #   step_metrics.start_time = datetime.now(timezone.utc).isoformat()
        #   try:
        #       results = extract_all()
        #       step_metrics.status = "success"
        #       step_metrics.rows_processed = sum(len(df) for df in results.values())
        #       step_metrics.tables_created = list(results.keys())
        #   except Exception as e:
        #       step_metrics.status = "failed"
        #       step_metrics.errors.append(str(e))
        #       raise
        #   finally:
        #       step_metrics.end_time = datetime.now(timezone.utc).isoformat()
        #       step_metrics.duration_seconds = round(time.time() - t0, 2)
        #       report.add_step(step_metrics)
        #extract_all()
        step_metrics = StepMetrics(step_name="extract")
        step_metrics.status = "running"
        step_metrics.start_time = datetime.now(timezone.utc).isoformat()
        try:
            results = extract_all()
            step_metrics.status = "success"
            step_metrics.rows_processed = sum(len(df) for df in results.values())
            step_metrics.tables_created = list(results.keys())
        except Exception as e:
            step_metrics.status = "failed"
            step_metrics.errors.append(str(e))
            raise
        finally:
            step_metrics.end_time = datetime.now(timezone.utc).isoformat()
            step_metrics.duration_seconds = round(time.time() - t0, 2)
            report.add_step(step_metrics)

    if step in ("all", "transform"):
        t1 = time.time()
        step_metrics = StepMetrics(step_name="transform")
        step_metrics.status = "running"
        step_metrics.start_time = datetime.now(timezone.utc).isoformat()
        try:
            results = transform_all()
            step_metrics.status = "success"
            step_metrics.rows_processed = sum(len(df) for df in results.values())
            step_metrics.tables_created = list(results.keys())
        except Exception as e:
            step_metrics.status = "failed"
            step_metrics.errors.append(str(e))
            raise
        finally:
            step_metrics.end_time = datetime.now(timezone.utc).isoformat()
            step_metrics.duration_seconds = round(time.time() - t1, 2)
            report.add_step(step_metrics)

        # TODO (TP4): Same pattern as extract — track with StepMetrics
        transform_all()

    if step in ("all", "gold"):
        # TODO (TP4): Same pattern as extract — track with StepMetrics
        step_metrics = StepMetrics(step_name="gold")
        step_metrics.status = "running"
        step_metrics.start_time = datetime.now(timezone.utc).isoformat()
        t2= time.time()
        try:
            create_gold_layer()
            step_metrics.status = "success"
        except Exception as e:
            step_metrics.status = "failed"
            step_metrics.errors.append(str(e))
            raise
        finally:
            step_metrics.end_time = datetime.now(timezone.utc).isoformat()
            step_metrics.duration_seconds = round(time.time() - t2, 2)
            report.add_step(step_metrics)

    elapsed = time.time() - t0
    print(f"\n{'=' * 60}")
    print(f"  ✅ Pipeline completed in {elapsed:.1f}s")
    print(f"{'=' * 60}")

    # TODO (TP4): Save the pipeline report to a JSON file
    #   report.save("pipeline_report.json")
    #   print(f"  📄 Report saved to pipeline_report.json")
    report.save("pipeline_report.json")
    print("  📄 Report saved to pipeline_report.json")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="KICKZ EMPIRE ELT Pipeline")
    parser.add_argument(
        "--step",
        choices=["all", "extract", "transform", "gold"],
        default="all",
        help="Pipeline step to execute (default: all)",
    )
    args = parser.parse_args()
    run_pipeline(step=args.step)
