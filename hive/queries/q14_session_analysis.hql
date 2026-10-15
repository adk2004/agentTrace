-- ====================================================================
-- Q14: Session analysis: sessions with most runs, avg runs per session, sessions where all runs failed
-- Question: What are the highest-activity user sessions, what is the average number
-- of runs per session, and which sessions experienced complete failure across all runs?
-- Dataset: runs_orc
-- ====================================================================

SET hive.execution.engine=mr;
SET mapreduce.job.reduces=2;
USE ai_obs;

WITH session_metrics AS (
    SELECT
        session_id,
        COUNT(*) AS runs_in_session,
        SUM(CASE WHEN LOWER(final_status) = 'success' THEN 1 ELSE 0 END) AS successful_runs,
        SUM(CASE WHEN LOWER(final_status) != 'success' THEN 1 ELSE 0 END) AS failed_runs,
        SUM(duration_ms) AS session_duration_ms,
        SUM(prompt_tokens_total + completion_tokens_total) AS session_total_tokens,
        CASE WHEN SUM(CASE WHEN LOWER(final_status) = 'success' THEN 1 ELSE 0 END) = 0 THEN 1 ELSE 0 END AS all_runs_failed
    FROM runs_orc
    WHERE session_id IS NOT NULL AND session_id != '' AND session_id != '\\N'
    GROUP BY session_id
)
SELECT
    session_id,
    runs_in_session,
    successful_runs,
    failed_runs,
    all_runs_failed,
    session_duration_ms,
    session_total_tokens,
    ROUND(AVG(runs_in_session) OVER (), 2) AS overall_avg_runs_per_session,
    SUM(all_runs_failed) OVER () AS total_sessions_all_failed,
    COUNT(*) OVER () AS total_sessions
FROM session_metrics
ORDER BY runs_in_session DESC, failed_runs DESC
LIMIT 50;
