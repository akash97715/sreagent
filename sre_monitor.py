"""
Dataiku SRE Monitoring Agent - Full Cycle Runner
Performs parallel log analysis across 10 Dataiku nodes via S3.
Falls back to simulation mode when AWS credentials are absent.
"""

import boto3
import json
import sys
import os
import concurrent.futures
import traceback
from datetime import datetime, timezone, timedelta
from botocore.exceptions import NoCredentialsError, ClientError

# ─── Configuration ─────────────────────────────────────────────────────────────
S3_BUCKET         = os.environ.get("DATAIKU_LOG_BUCKET", "dataiku-eventserver-logs-prod")
S3_PREFIX         = os.environ.get("DATAIKU_LOG_PREFIX", "dataiku/event-server/logs/")
AWS_REGION        = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
WINDOW_MINUTES    = 10
NODE_COUNT        = 10
NODE_IDS          = [f"node-{str(i).zfill(2)}" for i in range(1, NODE_COUNT + 1)]

# ─── Time Window ───────────────────────────────────────────────────────────────
NOW           = datetime.now(timezone.utc)
WINDOW_END    = NOW
WINDOW_START  = NOW - timedelta(minutes=WINDOW_MINUTES)
DATE_PATH     = NOW.strftime("%Y/%m/%d")
HOUR_PATH     = NOW.strftime("%H")

# ─── Simulated Log Data (used when AWS creds are absent) ───────────────────────
# Represents a realistic mixed-health cluster snapshot.
SIMULATED_NODE_LOGS = {
    "node-01": {
        "status": "HEALTHY",
        "events": [
            {"time": "2026-05-02T19:45:10Z", "level": "INFO",    "project": "SALES_PIPELINE",    "message": "Job COMPUTE_sales_metrics completed successfully in 42.3s"},
            {"time": "2026-05-02T19:47:22Z", "level": "INFO",    "project": "SALES_PIPELINE",    "message": "Scenario daily_refresh step 1/4 completed"},
            {"time": "2026-05-02T19:51:05Z", "level": "INFO",    "project": "INVENTORY_MGMT",    "message": "Recipe join_inventory_orders started"},
        ]
    },
    "node-02": {
        "status": "HEALTHY",
        "events": [
            {"time": "2026-05-02T19:44:00Z", "level": "INFO",    "project": "MARKETING_ANALYTICS","message": "Scenario weekly_cohort_analysis triggered by schedule"},
            {"time": "2026-05-02T19:48:33Z", "level": "INFO",    "project": "MARKETING_ANALYTICS","message": "Recipe compute_cohorts completed in 18.7s"},
            {"time": "2026-05-02T19:50:10Z", "level": "INFO",    "project": "MARKETING_ANALYTICS","message": "All scenario steps completed. Duration: 6m10s"},
        ]
    },
    "node-03": {
        "status": "CRITICAL",
        "events": [
            {"time": "2026-05-02T19:43:55Z", "level": "INFO",    "project": "FINANCE_ETL",        "message": "Scenario nightly_financial_close triggered by schedule"},
            {"time": "2026-05-02T19:44:30Z", "level": "INFO",    "project": "FINANCE_ETL",        "message": "Recipe transform_gl_entries started"},
            {"time": "2026-05-02T19:46:15Z", "level": "ERROR",   "project": "FINANCE_ETL",
             "message": "RecipeFailureException: Recipe 'transform_gl_entries' (Python) failed. "
                        "Traceback (most recent call last):\n"
                        "  File \"/dataiku/dss/recipes/FINANCE_ETL/transform_gl_entries.py\", line 87, in compute\n"
                        "    df = pd.merge(gl_df, chart_of_accounts, on='account_code', how='left', validate='m:1')\n"
                        "  File \"/usr/local/lib/python3.9/site-packages/pandas/core/reshape/merge.py\", line 108\n"
                        "MergeError: Merge keys are not unique in right dataset; not a many-to-one merge\n"
                        "[project: FINANCE_ETL] [recipe: transform_gl_entries] [type: Python]"},
            {"time": "2026-05-02T19:46:16Z", "level": "ERROR",   "project": "FINANCE_ETL",
             "message": "ScenarioAbortedException: Scenario 'nightly_financial_close' aborted at step 2 "
                        "'transform_gl_entries' due to recipe failure. Trigger: SCHEDULED. "
                        "Start: 2026-05-02T19:43:55Z Abort: 2026-05-02T19:46:16Z"},
        ]
    },
    "node-04": {
        "status": "WARNING",
        "events": [
            {"time": "2026-05-02T19:44:10Z", "level": "INFO",    "project": "CUSTOMER_360",       "message": "Recipe compute_customer_ltv started"},
            {"time": "2026-05-02T19:47:00Z", "level": "WARNING", "project": "CUSTOMER_360",
             "message": "Slow execution warning: Recipe 'compute_customer_ltv' (Spark) has been running for 180s "
                        "(threshold: 120s). Executor memory: 14.8GB / 16GB (92%). "
                        "[project: CUSTOMER_360] [recipe: compute_customer_ltv] [type: Spark]"},
            {"time": "2026-05-02T19:52:10Z", "level": "INFO",    "project": "CUSTOMER_360",       "message": "Recipe compute_customer_ltv completed in 487.2s"},
        ]
    },
    "node-05": {
        "status": "HEALTHY",
        "events": [
            {"time": "2026-05-02T19:45:00Z", "level": "INFO",    "project": "SUPPLY_CHAIN",       "message": "Recipe join_orders_shipments completed in 8.1s"},
            {"time": "2026-05-02T19:46:55Z", "level": "INFO",    "project": "SUPPLY_CHAIN",       "message": "Recipe forecast_demand started"},
            {"time": "2026-05-02T19:51:30Z", "level": "INFO",    "project": "SUPPLY_CHAIN",       "message": "Recipe forecast_demand completed in 275s"},
        ]
    },
    "node-06": {
        "status": "CRITICAL",
        "events": [
            {"time": "2026-05-02T19:43:00Z", "level": "INFO",    "project": "DATA_QUALITY",       "message": "Scenario dq_checks_hourly triggered by API"},
            {"time": "2026-05-02T19:43:45Z", "level": "ERROR",   "project": "DATA_QUALITY",
             "message": "JobFailedException: Job COMPUTE_dq_summary failed. "
                        "Caused by: OutOfMemoryError: Java heap space\n"
                        "  at org.apache.spark.sql.execution.SparkPlan.executeQuery(SparkPlan.scala:168)\n"
                        "  at org.apache.spark.sql.execution.SparkPlan.execute(SparkPlan.scala:127)\n"
                        "  at org.apache.spark.sql.execution.exchange.BroadcastExchangeExec.doPrepare(BroadcastExchangeExec.scala:93)\n"
                        "Executor lost: Container killed by YARN for exceeding physical memory limits. "
                        "16.4 GB of 16 GB physical memory used.\n"
                        "[project: DATA_QUALITY] [recipe: compute_dq_summary] [type: Spark]"},
            {"time": "2026-05-02T19:43:46Z", "level": "ERROR",   "project": "DATA_QUALITY",
             "message": "ScenarioAbortedException: Scenario 'dq_checks_hourly' aborted at step 1 "
                        "'compute_dq_summary' - SparkException: Job aborted due to stage failure. "
                        "Trigger: API. Start: 2026-05-02T19:43:00Z Abort: 2026-05-02T19:43:46Z"},
        ]
    },
    "node-07": {
        "status": "WARNING",
        "events": [
            {"time": "2026-05-02T19:44:45Z", "level": "INFO",    "project": "HR_ANALYTICS",       "message": "Recipe compute_headcount_trends started"},
            {"time": "2026-05-02T19:45:55Z", "level": "WARNING", "project": "HR_ANALYTICS",
             "message": "Disk space warning on executor node ip-10-0-3-45: 82% used (410GB / 500GB). "
                        "Recipe 'compute_headcount_trends' writing large intermediate dataset. "
                        "[project: HR_ANALYTICS] [recipe: compute_headcount_trends]"},
            {"time": "2026-05-02T19:48:10Z", "level": "INFO",    "project": "HR_ANALYTICS",       "message": "Recipe compute_headcount_trends completed in 205s"},
        ]
    },
    "node-08": {
        "status": "HEALTHY",
        "events": [
            {"time": "2026-05-02T19:46:00Z", "level": "INFO",    "project": "PRODUCT_ANALYTICS",  "message": "Scenario feature_engineering_daily triggered by schedule"},
            {"time": "2026-05-02T19:48:15Z", "level": "INFO",    "project": "PRODUCT_ANALYTICS",  "message": "Recipe feature_extraction_v2 completed in 135s"},
            {"time": "2026-05-02T19:50:45Z", "level": "INFO",    "project": "PRODUCT_ANALYTICS",  "message": "Scenario feature_engineering_daily completed. All 3 steps successful."},
        ]
    },
    "node-09": {
        "status": "HEALTHY",
        "events": [
            {"time": "2026-05-02T19:44:22Z", "level": "INFO",    "project": "RISK_SCORING",       "message": "Recipe score_credit_risk started"},
            {"time": "2026-05-02T19:47:58Z", "level": "INFO",    "project": "RISK_SCORING",       "message": "Recipe score_credit_risk completed in 216s. 142,850 records scored."},
            {"time": "2026-05-02T19:49:00Z", "level": "INFO",    "project": "RISK_SCORING",       "message": "Model serving endpoint refreshed with latest batch scores"},
        ]
    },
    "node-10": {
        "status": "WARNING",
        "events": [
            {"time": "2026-05-02T19:43:30Z", "level": "INFO",    "project": "REALTIME_INGEST",    "message": "Streaming recipe kafka_consumer_orders started"},
            {"time": "2026-05-02T19:49:45Z", "level": "WARNING", "project": "REALTIME_INGEST",
             "message": "ConnectionTimeout: Kafka broker kafka-broker-03.internal:9092 unreachable after 30s. "
                        "Retrying with fallback broker kafka-broker-04.internal:9092. "
                        "[project: REALTIME_INGEST] [recipe: kafka_consumer_orders]"},
            {"time": "2026-05-02T19:50:12Z", "level": "INFO",    "project": "REALTIME_INGEST",    "message": "Kafka consumer reconnected via fallback broker. Lag: 2,847 messages."},
        ]
    }
}


def build_s3_url(node_id):
    """Construct expected S3 URL for a node's log file in the current monitoring window."""
    return (
        f"s3://{S3_BUCKET}/{S3_PREFIX}{node_id}/"
        f"{DATE_PATH}/{HOUR_PATH}/"
        f"eventserver-{node_id}-{NOW.strftime('%Y%m%dT%H%M')}Z.json"
    )


def analyze_node(node_id, s3_client=None, simulation=True):
    """
    Analyze a single Dataiku node's event server logs.
    Returns a structured per-node analysis result.
    """
    s3_url = build_s3_url(node_id)
    result = {
        "node_id":       node_id,
        "s3_urls":       [s3_url],
        "log_window":    {"start": WINDOW_START.isoformat(), "end": WINDOW_END.isoformat()},
        "status":        "UNKNOWN",
        "event_count":   0,
        "error_count":   0,
        "warning_count": 0,
        "errors":        [],
        "warnings":      [],
        "recipe_failures":   [],
        "scenario_failures": [],
        "performance_metrics": {},
        "raw_error_snippets": [],
        "fetch_method":  "SIMULATED",
        "fetch_error":   None,
    }

    # ── Attempt live S3 fetch ──────────────────────────────────────────────────
    if s3_client and not simulation:
        try:
            # Extract bucket-relative key from URL
            key = s3_url.replace(f"s3://{S3_BUCKET}/", "")
            obj = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
            raw_data = json.loads(obj['Body'].read().decode('utf-8'))
            result["fetch_method"] = "S3_LIVE"
            log_events = raw_data.get("events", [])
        except ClientError as e:
            result["fetch_error"] = f"S3 ClientError: {e.response['Error']['Code']} - {e.response['Error']['Message']}"
            result["status"] = "UNKNOWN"
            result["fetch_method"] = "S3_FAILED"
            return result
        except Exception as e:
            result["fetch_error"] = f"Unexpected fetch error: {type(e).__name__}: {e}"
            result["status"] = "UNKNOWN"
            result["fetch_method"] = "S3_FAILED"
            return result
    else:
        # Simulation fallback
        sim = SIMULATED_NODE_LOGS.get(node_id, {"status": "HEALTHY", "events": []})
        log_events = sim["events"]

    # ── Parse events ──────────────────────────────────────────────────────────
    result["event_count"] = len(log_events)
    for ev in log_events:
        level   = ev.get("level", "INFO").upper()
        msg     = ev.get("message", "")
        project = ev.get("project", "UNKNOWN")
        ts      = ev.get("time", NOW.isoformat())

        if level == "ERROR" or level == "FATAL":
            result["error_count"] += 1
            result["raw_error_snippets"].append({"time": ts, "project": project, "message": msg[:500]})

            # Recipe failure detection
            if "RecipeFailureException" in msg or "recipe" in msg.lower():
                recipe_name = "UNKNOWN"
                recipe_type = "UNKNOWN"
                for part in msg.split():
                    if part.startswith("[recipe:"):
                        recipe_name = part.replace("[recipe:", "").rstrip("]")
                    if part.startswith("[type:"):
                        recipe_type = part.replace("[type:", "").rstrip("]")
                result["recipe_failures"].append({
                    "project":     project,
                    "recipe_name": recipe_name,
                    "recipe_type": recipe_type,
                    "time":        ts,
                    "message":     msg
                })

            # Scenario failure detection
            if "ScenarioAbortedException" in msg or "JobFailedException" in msg:
                scenario_name = "UNKNOWN"
                trigger       = "UNKNOWN"
                if "Scenario '" in msg:
                    try:
                        scenario_name = msg.split("Scenario '")[1].split("'")[0]
                    except IndexError:
                        pass
                if "Trigger:" in msg:
                    try:
                        trigger = msg.split("Trigger:")[1].split(".")[0].strip()
                    except IndexError:
                        pass
                result["scenario_failures"].append({
                    "project":       project,
                    "scenario_name": scenario_name,
                    "trigger":       trigger,
                    "time":          ts,
                    "message":       msg
                })

            result["errors"].append({"time": ts, "project": project, "level": level, "message": msg})

        elif level == "WARNING":
            result["warning_count"] += 1
            result["warnings"].append({"time": ts, "project": project, "message": msg})

        # Performance metrics: check for execution times
        if "completed in" in msg:
            try:
                duration_str = msg.split("completed in")[1].split("s")[0].strip()
                duration = float(duration_str)
                result["performance_metrics"][ev.get("project","?")] = f"{duration}s"
            except (ValueError, IndexError):
                pass

    # ── Derive final status ────────────────────────────────────────────────────
    if result["error_count"] > 0:
        result["status"] = "CRITICAL"
    elif result["warning_count"] > 0:
        result["status"] = "WARNING"
    else:
        result["status"] = "HEALTHY"

    return result


def run_parallel_analysis(simulation=True):
    """Spin up 10 parallel workers, one per node, and collect all results."""
    s3_client = None
    if not simulation:
        s3_client = boto3.client('s3', region_name=AWS_REGION)

    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=NODE_COUNT) as executor:
        futures = {
            executor.submit(analyze_node, node_id, s3_client, simulation): node_id
            for node_id in NODE_IDS
        }
        for future in concurrent.futures.as_completed(futures):
            node_id = futures[future]
            try:
                results[node_id] = future.result()
            except Exception as e:
                results[node_id] = {
                    "node_id":       node_id,
                    "s3_urls":       [build_s3_url(node_id)],
                    "status":        "UNKNOWN",
                    "error_count":   0,
                    "warning_count": 0,
                    "event_count":   0,
                    "errors":        [],
                    "warnings":      [],
                    "recipe_failures":   [],
                    "scenario_failures": [],
                    "performance_metrics": {},
                    "raw_error_snippets": [],
                    "fetch_error":   f"Subagent exception: {type(e).__name__}: {e}",
                    "log_window":    {"start": WINDOW_START.isoformat(), "end": WINDOW_END.isoformat()},
                }
    # Return ordered by node_id
    return {nid: results[nid] for nid in NODE_IDS}


def main():
    # Detect credentials
    simulation = True
    try:
        boto3.client('s3').list_buckets()
        simulation = False
        print("MODE=LIVE", file=sys.stderr)
    except NoCredentialsError:
        print("MODE=SIMULATION (no AWS credentials found)", file=sys.stderr)
    except Exception:
        print("MODE=SIMULATION (AWS unreachable)", file=sys.stderr)

    node_results = run_parallel_analysis(simulation=simulation)
    output = {
        "cycle_metadata": {
            "report_timestamp":   NOW.isoformat(),
            "window_start":       WINDOW_START.isoformat(),
            "window_end":         WINDOW_END.isoformat(),
            "s3_bucket":          S3_BUCKET,
            "s3_prefix":          S3_PREFIX,
            "aws_region":         AWS_REGION,
            "node_count":         NODE_COUNT,
            "simulation_mode":    simulation,
        },
        "node_results": node_results
    }
    print(json.dumps(output, indent=2))


if __name__ == "__main__":
    main()
