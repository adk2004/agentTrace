package com.aitrace.job.analytics.latency;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class LatencyHistogramCombiner extends Reducer<Text, LatencyStatsWritable, Text, LatencyStatsWritable> {
    @Override
    protected void reduce(Text key, Iterable<LatencyStatsWritable> values, Context context)
            throws IOException, InterruptedException {
        LatencyStatsWritable result = new LatencyStatsWritable();
        for (LatencyStatsWritable val : values) {
            result.merge(val);
        }
        context.write(key, result);
    }
}
