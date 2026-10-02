package com.aitrace.job.analytics.tool;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class ToolStatsCombiner extends Reducer<Text, ToolStatsWritable, Text, ToolStatsWritable> {
    @Override
    protected void reduce(Text key, Iterable<ToolStatsWritable> values, Context context)
            throws IOException, InterruptedException {
        ToolStatsWritable result = new ToolStatsWritable();
        for (ToolStatsWritable val : values) {
            result.merge(val);
        }
        context.write(key, result);
    }
}
