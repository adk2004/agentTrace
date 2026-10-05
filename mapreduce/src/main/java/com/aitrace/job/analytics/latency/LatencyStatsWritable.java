package com.aitrace.job.analytics.latency;

import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LatencyStatsWritable implements Writable {
    private long count;
    private long min;
    private long max;
    private long sum;

    public LatencyStatsWritable() {}

    public LatencyStatsWritable(long count, long min, long max, long sum) {
        this.count = count;
        this.min = min;
        this.max = max;
        this.sum = sum;
    }

    public void merge(LatencyStatsWritable other) {
        if (this.count == 0) {
            this.count = other.count;
            this.min = other.min;
            this.max = other.max;
            this.sum = other.sum;
        } else if (other.count > 0) {
            this.count += other.count;
            this.min = Math.min(this.min, other.min);
            this.max = Math.max(this.max, other.max);
            this.sum += other.sum;
        }
    }

    public long getCount() { return count; }
    public long getMin() { return min; }
    public long getMax() { return max; }
    public long getSum() { return sum; }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(count);
        out.writeLong(min);
        out.writeLong(max);
        out.writeLong(sum);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        count = in.readLong();
        min = in.readLong();
        max = in.readLong();
        sum = in.readLong();
    }
}
