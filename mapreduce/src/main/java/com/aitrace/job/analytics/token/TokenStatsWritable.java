package com.aitrace.job.analytics.token;

import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TokenStatsWritable implements Writable {
    private long llmCalls;
    private long sumPrompt;
    private long sumCompletion;
    private long maxPrompt;
    private long maxCompletion;
    private long tokens0_1;
    private long tokens2_4;
    private long tokens5_9;
    private long tokens10Plus;

    public TokenStatsWritable() {}

    public TokenStatsWritable(long llmCalls, long prompt, long completion, int stepIndex) {
        this.llmCalls = llmCalls;
        this.sumPrompt = prompt;
        this.sumCompletion = completion;
        this.maxPrompt = prompt;
        this.maxCompletion = completion;
        long totalTokens = prompt + completion;
        if (stepIndex <= 1) {
            this.tokens0_1 = totalTokens;
        } else if (stepIndex <= 4) {
            this.tokens2_4 = totalTokens;
        } else if (stepIndex <= 9) {
            this.tokens5_9 = totalTokens;
        } else {
            this.tokens10Plus = totalTokens;
        }
    }

    public void merge(TokenStatsWritable other) {
        this.llmCalls += other.llmCalls;
        this.sumPrompt += other.sumPrompt;
        this.sumCompletion += other.sumCompletion;
        this.maxPrompt = Math.max(this.maxPrompt, other.maxPrompt);
        this.maxCompletion = Math.max(this.maxCompletion, other.maxCompletion);
        this.tokens0_1 += other.tokens0_1;
        this.tokens2_4 += other.tokens2_4;
        this.tokens5_9 += other.tokens5_9;
        this.tokens10Plus += other.tokens10Plus;
    }

    public long getLlmCalls() { return llmCalls; }
    public long getSumPrompt() { return sumPrompt; }
    public long getSumCompletion() { return sumCompletion; }
    public long getMaxPrompt() { return maxPrompt; }
    public long getMaxCompletion() { return maxCompletion; }
    public long getTokens0_1() { return tokens0_1; }
    public long getTokens2_4() { return tokens2_4; }
    public long getTokens5_9() { return tokens5_9; }
    public long getTokens10Plus() { return tokens10Plus; }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(llmCalls);
        out.writeLong(sumPrompt);
        out.writeLong(sumCompletion);
        out.writeLong(maxPrompt);
        out.writeLong(maxCompletion);
        out.writeLong(tokens0_1);
        out.writeLong(tokens2_4);
        out.writeLong(tokens5_9);
        out.writeLong(tokens10Plus);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        llmCalls = in.readLong();
        sumPrompt = in.readLong();
        sumCompletion = in.readLong();
        maxPrompt = in.readLong();
        maxCompletion = in.readLong();
        tokens0_1 = in.readLong();
        tokens2_4 = in.readLong();
        tokens5_9 = in.readLong();
        tokens10Plus = in.readLong();
    }
}
