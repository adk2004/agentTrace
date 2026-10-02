package com.aitrace.job.analytics.tool;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.*;

public class ToolStatsReducer extends Reducer<Text, ToolStatsWritable, NullWritable, Text> {
    public enum Counters { UNIQUE_TOOL_TASKS, TOTAL_CALLS }
    private Text outValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<ToolStatsWritable> values, Context context)
            throws IOException, InterruptedException {
        ToolStatsWritable agg = new ToolStatsWritable();
        for (ToolStatsWritable val : values) {
            agg.merge(val);
        }

        context.getCounter(Counters.UNIQUE_TOOL_TASKS).increment(1);
        context.getCounter(Counters.TOTAL_CALLS).increment(agg.getCalls());

        long calls = agg.getCalls();
        long ok = agg.getOk();
        long errors = agg.getErrors();
        long timeouts = agg.getTimeouts();
        double failureRate = calls > 0 ? (double)(errors + timeouts) / calls : 0.0;
        double avgLatency = calls > 0 ? (double)agg.getTotalLatencyMs() / calls : 0.0;
        double avgRetries = calls > 0 ? (double)agg.getTotalRetries() / calls : 0.0;

        String topErrors = formatTopErrors(agg.getErrorCounts());

        // Output: tool_name|task_type \t calls \t ok \t errors \t timeouts \t failure_rate \t avg_latency_ms \t avg_retries \t top_error_types
        String row = String.format(Locale.ROOT,
            "%s\t%d\t%d\t%d\t%d\t%.4f\t%.2f\t%.2f\t%s",
            key.toString(), calls, ok, errors, timeouts, failureRate, avgLatency, avgRetries, topErrors);

        outValue.set(row);
        context.write(NullWritable.get(), outValue);
    }

    private String formatTopErrors(Map<String, Long> errorCounts) {
        if (errorCounts == null || errorCounts.isEmpty()) return "\\N";
        List<Map.Entry<String, Long>> list = new ArrayList<>(errorCounts.entrySet());
        list.sort((a, b) -> Long.compare(b.getValue(), a.getValue()));
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < list.size(); i++) {
            if (i > 0) sb.append(";");
            sb.append(list.get(i).getKey()).append(":").append(list.get(i).getValue());
        }
        return sb.toString();
    }
}
