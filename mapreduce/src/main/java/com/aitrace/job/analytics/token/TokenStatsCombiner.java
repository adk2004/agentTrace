package com.aitrace.job.analytics.token;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class TokenStatsCombiner extends Reducer<Text, TokenStatsWritable, Text, TokenStatsWritable> {
    @Override
    protected void reduce(Text key, Iterable<TokenStatsWritable> values, Context context)
            throws IOException, InterruptedException {
        TokenStatsWritable result = new TokenStatsWritable();
        for (TokenStatsWritable val : values) {
            result.merge(val);
        }
        context.write(key, result);
    }
}
