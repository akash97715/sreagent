# Dataiku SRE Monitor - Persistent Agent Memory

## Environment Facts
- Working dir: /Users/akasdeep/Desktop/claudedataiku
- Python: /Library/Frameworks/Python.framework/Versions/3.11/bin/python3 (3.11.9)
- boto3: installed at version 1.43.2 via pip3
- AWS CLI: not installed on this machine
- AWS credentials: CONFIRMED ABSENT (verified 2026-05-03 via real STS call)
  - Gate 1: boto3 STS get_caller_identity() -> NoCredentialsError (no HTTP call made)
  - Gate 2: No AWS/Dataiku env vars present
  - Gate 3: ~/.aws/ directory does not exist
  - Gate 4: EC2 IMDS 169.254.169.254 and ECS 169.254.170.2 both unreachable (timeout)
- Fetch mode defaults to SIMULATION when credentials are absent

## Activation Gate Sequence (run on every cycle)
1. boto3 sts.get_caller_identity() -- expect SAML assumed-role ARN
2. Env var scan (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN, etc.)
3. ~/.aws/credentials + ~/.aws/config file probe
4. EC2/ECS IMDS endpoint probe (1s timeout each)
All four blocked -> SIMULATION mode. Any gate passes -> attempt live S3 fetch.

## S3 Configuration (defaults, override via env vars)
- DATAIKU_LOG_BUCKET : dataiku-eventserver-logs-prod
- DATAIKU_LOG_PREFIX : dataiku/event-server/logs/
- AWS_DEFAULT_REGION : us-east-1
- Log file naming pattern: eventserver-{node_id}-{YYYYMMDDTHHmm}Z.json
- S3 path pattern: s3://{bucket}/{prefix}{node_id}/{YYYY}/{MM}/{DD}/{HH}/{filename}

## Key Scripts
- /Users/akasdeep/Desktop/claudedataiku/sre_monitor.py  -- main monitor runner
  - Parallel (ThreadPoolExecutor, 10 workers) per-node analysis
  - Outputs JSON to stdout; use: python3 sre_monitor.py > output.json
  - Falls back to SIMULATION mode if NoCredentialsError is raised
  - NODE_IDS: node-01 through node-10

## Node Baseline (confirmed across 2026-05-02 and 2026-05-03 cycles)
- Typical event count per node per 10-min window: 3-4
- Normal error rate: 0 per cycle (any error is significant)
- See patterns.md for per-project baselines

## Recurring Issues Observed (simulation baseline, consistent across cycles)
- node-03 / FINANCE_ETL: pandas MergeError on chart_of_accounts duplicates
  -> nightly_financial_close scenario abort (SCHEDULED trigger)
  -> Root fix: deduplicate account_code in source; add DQ check upstream
- node-06 / DATA_QUALITY: Spark OOM (16GB heap exceeded, YARN container kill)
  -> dq_checks_hourly scenario abort (API trigger)
  -> Root fix: set autoBroadcastJoinThreshold=-1; raise executor memoryOverhead
- node-04 / CUSTOMER_360: Spark memory at 92%, slow LTV recipe (487s)
  -> Pre-OOM warning; same Spark memory tuning as node-06 applies
- node-07 / HR_ANALYTICS: Disk 82% on ip-10-0-3-45
  -> Clear managed folder cache; extend EBS before next run
- node-10 / REALTIME_INGEST: Kafka broker-03 connection timeout with failover
  -> Failover to broker-04 succeeds; investigate broker-03 health

## Output Files
- Daily health report: /Users/akasdeep/Desktop/claudedataiku/health_report_YYYY-MM-DD.txt
- Raw JSON results  : /Users/akasdeep/Desktop/claudedataiku/sre_monitor_output.json
- See patterns.md for error signatures and remediation shortcuts

## Credential Setup (to enable live mode)
  export AWS_ACCESS_KEY_ID=...
  export AWS_SECRET_ACCESS_KEY=...
  export DATAIKU_LOG_BUCKET=<real_bucket>
  Required IAM: s3:ListBucket + s3:GetObject on log bucket
