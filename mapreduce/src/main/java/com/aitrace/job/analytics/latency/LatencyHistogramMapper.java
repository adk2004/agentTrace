package com.aitrace.job.analytics.latency;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class LatencyHistogramMapper extends Mapper<LongWritable, Text, Text, LatencyStatsWritable> {
    public enum Counters { TOTAL_LINES, RECORDS_PROCESSED, INVALID_LATENCY }

    private Text outKey = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        context.getCounter(Counters.TOTAL_LINES).increment(1);
        String line = value.toString();
        if (line.isEmpty()) return;

        String[] fields = line.split("\t", -1);
        if (fields.length < 22) return;

        String model = nullIf(fields[5], "unknown");
        String stepType = nullIf(fields[11], "unknown");

        long latency;
        try {
            latency = Long.parseLong(fields[14]);
        } catch (Exception e) {
            context.getCounter(Counters.INVALID_LATENCY).increment(1);
            return;
        }

        if (latency < 0) {
            latency = 0;
        }

        String bucket = getBucket(latency);

        context.getCounter(Counters.RECORDS_PROCESSED).increment(1);
        outKey.set(model + "|" + stepType + "\t" + bucket);
        LatencyStatsWritable outVal = new LatencyStatsWritable(1, latency, latency, latency);
        context.write(outKey, outVal);
    }

    public static String getBucket(long latency) {
        if (latency < 100) return "<100";
        if (latency < 250) return "100-250";
        if (latency < 500) return "250-500";
        if (latency < 1000) return "500-1000";
        if (latency < 2500) return "1000-2500";
        if (latency < 5000) return "2500-5000";
        if (latency < 10000) return "5000-10000";
        return ">=10000";
    }

    private static String nullIf(String s, String def) {
        if (s == null || s.isEmpty() || "\\N".equals(s)) return def;
        return s;
    }
}
