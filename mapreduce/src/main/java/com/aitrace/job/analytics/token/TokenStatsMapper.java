package com.aitrace.job.analytics.token;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class TokenStatsMapper extends Mapper<LongWritable, Text, Text, TokenStatsWritable> {
    public enum Counters { TOTAL_LINES, LLM_CALLS_PROCESSED, SKIPPED_NON_LLM }

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
        if (!"llm_call".equals(stepType)) {
            context.getCounter(Counters.SKIPPED_NON_LLM).increment(1);
            return;
        }

        // Column 4: task_type, Column 5: model, Column 10: step_index, Column 13: dt
        String taskType = nullIf(fields[4], "unknown");
        String model    = nullIf(fields[5], "unknown");
        String dt       = nullIf(fields[13], "unknown");

        int stepIndex = (int) parseLong(fields[10], 0L);
        long promptTokens = parseLong(fields[15], 0L);
        long completionTokens = parseLong(fields[16], 0L);

        context.getCounter(Counters.LLM_CALLS_PROCESSED).increment(1);
        outKey.set(model + "|" + taskType + "|" + dt);
        TokenStatsWritable outVal = new TokenStatsWritable(1, promptTokens, completionTokens, stepIndex);
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
