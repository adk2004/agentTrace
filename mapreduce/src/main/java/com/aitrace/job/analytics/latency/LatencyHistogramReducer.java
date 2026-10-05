package com.aitrace.job.analytics.latency;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Locale;

public class LatencyHistogramReducer extends Reducer<Text, LatencyStatsWritable, NullWritable, Text> {
    public enum Counters { TOTAL_BUCKETS, TOTAL_RECORDS }
    private Text outValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<LatencyStatsWritable> values, Context context)
            throws IOException, InterruptedException {
        LatencyStatsWritable agg = new LatencyStatsWritable();
        for (LatencyStatsWritable val : values) {
            agg.merge(val);
        }

        context.getCounter(Counters.TOTAL_BUCKETS).increment(1);
        context.getCounter(Counters.TOTAL_RECORDS).increment(agg.getCount());

        long count = agg.getCount();
        long min = agg.getMin();
        long max = agg.getMax();
        long sum = agg.getSum();
        double avg = count > 0 ? (double) sum / count : 0.0;

        // Key is already "model|step_type\tbucket"
        // Output: model|step_type \t bucket \t count \t min \t max \t avg \t sum
        String row = String.format(Locale.ROOT,
            "%s\t%d\t%d\t%d\t%.2f\t%d",
            key.toString(), count, min, max, avg, sum);

        outValue.set(row);
        context.write(NullWritable.get(), outValue);
    }
}
