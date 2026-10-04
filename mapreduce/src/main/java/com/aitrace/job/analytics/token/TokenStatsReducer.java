package com.aitrace.job.analytics.token;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Locale;

public class TokenStatsReducer extends Reducer<Text, TokenStatsWritable, NullWritable, Text> {
    public enum Counters { UNIQUE_MODEL_TASKS, TOTAL_LLM_CALLS }
    private Text outValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<TokenStatsWritable> values, Context context)
            throws IOException, InterruptedException {
        TokenStatsWritable agg = new TokenStatsWritable();
        for (TokenStatsWritable val : values) {
            agg.merge(val);
        }

        context.getCounter(Counters.UNIQUE_MODEL_TASKS).increment(1);
        context.getCounter(Counters.TOTAL_LLM_CALLS).increment(agg.getLlmCalls());

        long calls = agg.getLlmCalls();
        long sumPrompt = agg.getSumPrompt();
        long sumCompletion = agg.getSumCompletion();
        double avgPrompt = calls > 0 ? (double) sumPrompt / calls : 0.0;
        double avgCompletion = calls > 0 ? (double) sumCompletion / calls : 0.0;
        long maxPrompt = agg.getMaxPrompt();
        long maxCompletion = agg.getMaxCompletion();

        // Output: model|task_type|dt \t llm_calls \t sum_prompt \t sum_completion \t avg_prompt \t avg_completion \t max_prompt \t max_completion \t tokens_0_1 \t tokens_2_4 \t tokens_5_9 \t tokens_10_plus
        String row = String.format(Locale.ROOT,
            "%s\t%d\t%d\t%d\t%.2f\t%.2f\t%d\t%d\t%d\t%d\t%d\t%d",
            key.toString(), calls, sumPrompt, sumCompletion, avgPrompt, avgCompletion,
            maxPrompt, maxCompletion, agg.getTokens0_1(), agg.getTokens2_4(), agg.getTokens5_9(), agg.getTokens10Plus());

        outValue.set(row);
        context.write(NullWritable.get(), outValue);
    }
}
