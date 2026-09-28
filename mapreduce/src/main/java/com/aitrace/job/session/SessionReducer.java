package com.aitrace.job.session;

import com.aitrace.util.TsvUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class SessionReducer extends Reducer<RunTimeKey, Text, NullWritable, Text> {
    public enum RunCounters { RUNS_BUILT, ORPHAN_EVENTS, DUPLICATE_SPANS, LONG_RUNS }

    static class SessionSummary {
        int runsInSession = 0;
        long totalTokens = 0;
        long totalDurationMs = 0;
    }

    private Text outRow = new Text();
    MultipleOutputs<NullWritable, Text> mos;
    private Map<String, SessionSummary> sessionMap = new HashMap<>();

    @Override
    protected void setup(Context ctx) {
        if (mos == null) {
            mos = new MultipleOutputs<>(ctx);
        }
    }

    @Override
    protected void reduce(RunTimeKey key, Iterable<Text> values, Context ctx)
            throws IOException, InterruptedException {
        // --- per-run streaming accumulators (no buffering of steps) ---
        String runId      = key.getRunId();
        String sessionId  = null, agentName = null, taskType = null, model = null, finalStatus = null;
        long   startTs    = Long.MAX_VALUE, endTs = Long.MIN_VALUE;
        long   totalLatency = 0, maxLatency = 0;
        int    totalSteps = 0, llmCalls = 0, toolCalls = 0;
        int    toolFailures = 0, toolTimeouts = 0, totalRetries = 0;
        long   promptTotal = 0, completionTotal = 0;
        String firstErrorType = null;
        Set<String> distinctTools = new HashSet<>();
        Set<String> seenSpans     = new HashSet<>();
        String prevSpanId = null;

        // retry-loop detection: same tool failed >= 3 times consecutively
        String lastFailTool = null;
        int consecutiveFail = 0;
        boolean isRetryLoop = false;

        for (Text val : values) {
            totalSteps++;
            String[] f = val.toString().split("\t", -1);
            if (f.length < 22) {
                ctx.getCounter(RunCounters.ORPHAN_EVENTS).increment(1);
                totalSteps--;
                continue;
            }

            String spanId = nullIf(f[0]);
            // deduplicate by span_id using previous span or bounded set
            if (spanId != null) {
                if (spanId.equals(prevSpanId) || seenSpans.contains(spanId)) {
                    ctx.getCounter(RunCounters.DUPLICATE_SPANS).increment(1);
                    totalSteps--;
                    continue;
                }
                prevSpanId = spanId;
                if (seenSpans.size() < 500) {
                    seenSpans.add(spanId);
                }
            }

            if (sessionId  == null) sessionId  = nullIf(f[1]);
            if (agentName  == null) agentName  = nullIf(f[2]);
            if (taskType   == null) taskType   = nullIf(f[3]);
            if (model      == null) model      = nullIf(f[4]);
            if (nullIf(f[5]) != null) finalStatus = f[5]; // latest seen status

            // timestamps (secondary sorted, but min/max robust)
            long ts = parseLong(f[8], 0L);
            if (ts > 0) {
                if (ts < startTs) startTs = ts;
                if (ts > endTs) endTs = ts;
            }

            // latency
            long lat = parseLong(f[9], 0L);
            totalLatency += lat;
            if (lat > maxLatency) maxLatency = lat;

            // event type based counters
            String evType = nullIf(f[7]);
            if ("llm_call".equals(evType)) llmCalls++;
            if ("tool_call".equals(evType)) toolCalls++;

            // tool fields
            String toolName   = nullIf(f[13]);
            String toolStatus = nullIf(f[14]);
            String errType    = nullIf(f[15]);
            int    retries    = (int) parseLong(f[16], 0L);
            totalRetries += retries;

            if (toolName != null) distinctTools.add(toolName);
            if ("failed".equals(toolStatus))  toolFailures++;
            if ("timeout".equals(toolStatus)) toolTimeouts++;

            // first error type
            if (errType != null && firstErrorType == null) firstErrorType = errType;

            // retry-loop: same tool failed >= 3 consecutive times
            if ("tool_call".equals(evType) || toolName != null) {
                if ("failed".equals(toolStatus)) {
                    if (toolName != null && toolName.equals(lastFailTool)) {
                        consecutiveFail++;
                    } else {
                        lastFailTool = toolName;
                        consecutiveFail = 1;
                    }
                    if (consecutiveFail >= 3) {
                        isRetryLoop = true;
                    }
                } else if ("ok".equals(toolStatus) || "success".equals(toolStatus)) {
                    lastFailTool = null;
                    consecutiveFail = 0;
                } else if (toolName != null && !toolName.equals(lastFailTool)) {
                    lastFailTool = null;
                    consecutiveFail = 0;
                }
            }

            // tokens
            promptTotal     += parseLong(f[10], 0L);
            completionTotal += parseLong(f[11], 0L);
        }

        if (totalSteps <= 0) {
            ctx.getCounter(RunCounters.ORPHAN_EVENTS).increment(1);
            return;
        }

        long durationMs = (endTs == Long.MIN_VALUE || startTs == Long.MAX_VALUE) ? 0L : Math.max(0L, endTs - startTs);
        if (totalSteps > 500) ctx.getCounter(RunCounters.LONG_RUNS).increment(1);
        ctx.getCounter(RunCounters.RUNS_BUILT).increment(1);

        // ---- emit run row ----
        // run_id, session_id, agent_name, task_type, model, start_ts, end_ts, duration_ms,
        // total_steps, llm_calls, tool_calls, tool_failures, tool_timeouts, total_retries,
        // prompt_tokens_total, completion_tokens_total, total_latency_ms, max_step_latency_ms,
        // distinct_tools_used, first_error_type, final_status, is_retry_loop
        StringBuilder sb = new StringBuilder();
        sb.append(TsvUtil.escape(runId)).append('\t')
          .append(TsvUtil.escape(sessionId)).append('\t')
          .append(TsvUtil.escape(agentName)).append('\t')
          .append(TsvUtil.escape(taskType)).append('\t')
          .append(TsvUtil.escape(model)).append('\t')
          .append(startTs == Long.MAX_VALUE ? "\\N" : Long.toString(startTs)).append('\t')
          .append(endTs   == Long.MIN_VALUE ? "\\N" : Long.toString(endTs)).append('\t')
          .append(durationMs).append('\t')
          .append(totalSteps).append('\t')
          .append(llmCalls).append('\t')
          .append(toolCalls).append('\t')
          .append(toolFailures).append('\t')
          .append(toolTimeouts).append('\t')
          .append(totalRetries).append('\t')
          .append(promptTotal).append('\t')
          .append(completionTotal).append('\t')
          .append(totalLatency).append('\t')
          .append(maxLatency).append('\t')
          .append(distinctTools.size()).append('\t')
          .append(TsvUtil.escape(firstErrorType)).append('\t')
          .append(TsvUtil.escape(finalStatus)).append('\t')
          .append(isRetryLoop ? "1" : "0");
        outRow.set(sb.toString());
        ctx.write(NullWritable.get(), outRow);

        // ---- accumulate session-level rollup ----
        if (sessionId != null) {
            SessionSummary summary = sessionMap.computeIfAbsent(sessionId, k -> new SessionSummary());
            summary.runsInSession++;
            summary.totalTokens += (promptTotal + completionTotal);
            summary.totalDurationMs += durationMs;
        }
    }

    @Override
    protected void cleanup(Context ctx) throws IOException, InterruptedException {
        if (mos != null) {
            for (Map.Entry<String, SessionSummary> entry : sessionMap.entrySet()) {
                SessionSummary s = entry.getValue();
                String sessionRow = entry.getKey() + "\t" + s.runsInSession + "\t" + s.totalTokens + "\t" + s.totalDurationMs;
                mos.write("session", NullWritable.get(), new Text(sessionRow), "session_rollup/part");
            }
            mos.close();
        }
    }

    private static String nullIf(String s) {
        return (s == null || "\\N".equals(s) || s.isEmpty()) ? null : s;
    }

    private static long parseLong(String s, long def) {
        try {
            return Long.parseLong(s);
        } catch (Exception e) {
            return def;
        }
    }
}
