# Dataiku SRE - Error Patterns and Remediation Reference

## Pattern 1: pandas MergeError on validate='m:1'
Log signature : MergeError: Merge keys are not unique in right dataset; not a many-to-one merge
Project seen  : FINANCE_ETL
Recipe        : transform_gl_entries (Python)
Root cause    : Duplicate values in the reference/dimension table join key
Fix           : Deduplicate reference dataset; add DQ check gating scenario trigger
Quick bypass  : Change validate='m:1' -> validate=None temporarily

## Pattern 2: Spark OOM / YARN Container Kill
Log signature : OutOfMemoryError: Java heap space + "Container killed by YARN for exceeding physical memory"
Stack marker  : BroadcastExchangeExec.doPrepare  (indicates broadcast join materializing in heap)
Project seen  : DATA_QUALITY
Recipe        : compute_dq_summary (Spark)
Root cause    : Broadcast join threshold too high or data volume growth exceeds executor memory
Fix           : Set spark.sql.autoBroadcastJoinThreshold=-1; raise spark.executor.memory; add memoryOverhead

## Pattern 3: Spark Slow Execution + High Memory Warning
Log signature : "Slow execution warning" + "Executor memory: X GB / 16 GB (92%)"
Project seen  : CUSTOMER_360
Recipe        : compute_customer_ltv (Spark)
Risk          : Precursor to OOM - same pattern as DATA_QUALITY before it fails
Fix           : Raise memory allocation proactively; optimize with AQE/repartitioning

## Pattern 4: Disk Space Warning on Executor Node
Log signature : "Disk space warning on executor node ip-10-0-3-45: 82%"
Project seen  : HR_ANALYTICS
Recipe        : compute_headcount_trends
Fix           : Clear /tmp/spark-* stale shuffles; archive old logs to S3; expand volume

## Pattern 5: Kafka Broker Connection Timeout with Failover
Log signature : "ConnectionTimeout: Kafka broker ... unreachable after 30s. Retrying with fallback"
Project seen  : REALTIME_INGEST
Recipe        : kafka_consumer_orders
Recovery      : Automatic failover to secondary broker succeeded
Check         : Investigate broker-03 health; monitor consumer lag after reconnect

## Severity Escalation Rules
- Any ScenarioAbortedException on a FINANCE_* project -> page Finance Data Engineering immediately
- Spark OOM on DATA_QUALITY -> downstream data quality gates will be missing; notify dependent teams
- Kafka failover lag > 5000 messages -> escalate to Platform/Infra team
- Disk > 90% -> CRITICAL, immediate cleanup required

## Project Criticality Reference (observed)
HIGH    : FINANCE_ETL (financial close), DATA_QUALITY (DQ gating), REALTIME_INGEST (orders)
MEDIUM  : CUSTOMER_360 (LTV scoring), RISK_SCORING (credit), SUPPLY_CHAIN (forecasting)
LOWER   : MARKETING_ANALYTICS, PRODUCT_ANALYTICS, HR_ANALYTICS, SALES_PIPELINE
