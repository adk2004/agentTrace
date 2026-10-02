package com.aitrace.job.analytics.tool;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class ToolStatsMapper extends Mapper<LongWritable, Text, Text, ToolStatsWritable> {
    public enum Counters { TOTAL_LINES, TOOL_CALLS_PROCESSED, SKIPPED_NON_TOOL_CALL }

    private Text outKey = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        context.getCounter(Counters.TOTAL_LINES).increment(1);
        String line = value.toString();
        if (line.isEmpty()) return;

        String[] fields = line.split("\t", -1);
        if (fields.length < 22) return;

        // Column 11: step_type
        String stepType = fields[11];
        if (!"tool_call".equals(stepType)) {
            context.getCounter(Counters.SKIPPED_NON_TOOL_CALL).increment(1);
            return;
        }

        // Column 4: task_type, Column 18: tool_name
        String taskType = nullIf(fields[4], "unknown");
        String toolName = nullIf(fields[18], null);
        if (toolName == null) {
            context.getCounter(Counters.SKIPPED_NON_TOOL_CALL).increment(1);
            return;
        }

        long latency = parseLong(fields[14], 0L);
        String toolStatus = fields[19];
        String errorType = nullIf(fields[20], null);
        long retries = parseLong(fields[21], 0L);

        long ok = 0, errors = 0, timeouts = 0;
        if ("ok".equalsIgnoreCase(toolStatus) || "success".equalsIgnoreCase(toolStatus)) {
            ok = 1;
        } else if ("timeout".equalsIgnoreCase(toolStatus)) {
            timeouts = 1;
        } else {
            errors = 1;
        }

        context.getCounter(Counters.TOOL_CALLS_PROCESSED).increment(1);
        outKey.set(toolName + "|" + taskType);
        ToolStatsWritable outVal = new ToolStatsWritable(1, ok, errors, timeouts, latency, retries, errorType);
        context.write(outKey, outVal);
    }

    private static String nullIf(String s, String def) {
        if (s == null || s.isEmpty() || "\\N".equals(s)) return def;
        return s;
    }

    private static long parseLong(String s, long def) {
        try {
            return Long.parseLong(s);
        } catch (Exception e) {
            return def;
        }
    }
}
