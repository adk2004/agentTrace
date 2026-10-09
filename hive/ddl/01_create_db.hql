-- ====================================================================
-- 01_create_db.hql
-- Create Hive database for AI Agent Observability warehouse
-- ====================================================================

SET hive.execution.engine=mr;
SET mapreduce.job.reduces=2;

CREATE DATABASE IF NOT EXISTS ai_obs
COMMENT 'AI Agent Execution Traces and LLM Telemetry Warehouse';

