package com.aitrace.job.analytics.tool;

import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ToolStatsWritable implements Writable {
    private long calls;
    private long ok;
    private long errors;
    private long timeouts;
    private long totalLatencyMs;
    private long totalRetries;
    private Map<String, Long> errorCounts = new HashMap<>();

    public ToolStatsWritable() {}

    public ToolStatsWritable(long calls, long ok, long errors, long timeouts,
                             long totalLatencyMs, long totalRetries, String errorType) {
        this.calls = calls;
        this.ok = ok;
        this.errors = errors;
        this.timeouts = timeouts;
        this.totalLatencyMs = totalLatencyMs;
        this.totalRetries = totalRetries;
        if (errorType != null && !errorType.isEmpty() && !"\\N".equals(errorType)) {
            this.errorCounts.put(errorType, 1L);
        }
    }

    public void merge(ToolStatsWritable other) {
        this.calls += other.calls;
        this.ok += other.ok;
        this.errors += other.errors;
        this.timeouts += other.timeouts;
        this.totalLatencyMs += other.totalLatencyMs;
        this.totalRetries += other.totalRetries;
        for (Map.Entry<String, Long> entry : other.errorCounts.entrySet()) {
            this.errorCounts.put(entry.getKey(),
                this.errorCounts.getOrDefault(entry.getKey(), 0L) + entry.getValue());
        }
    }

    public long getCalls() { return calls; }
    public long getOk() { return ok; }
    public long getErrors() { return errors; }
    public long getTimeouts() { return timeouts; }
    public long getTotalLatencyMs() { return totalLatencyMs; }
    public long getTotalRetries() { return totalRetries; }
    public Map<String, Long> getErrorCounts() { return errorCounts; }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(calls);
        out.writeLong(ok);
        out.writeLong(errors);
        out.writeLong(timeouts);
        out.writeLong(totalLatencyMs);
        out.writeLong(totalRetries);
        out.writeInt(errorCounts.size());
        for (Map.Entry<String, Long> e : errorCounts.entrySet()) {
            out.writeUTF(e.getKey());
            out.writeLong(e.getValue());
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        calls = in.readLong();
        ok = in.readLong();
        errors = in.readLong();
        timeouts = in.readLong();
        totalLatencyMs = in.readLong();
        totalRetries = in.readLong();
        int size = in.readInt();
        errorCounts.clear();
        for (int i = 0; i < size; i++) {
            String k = in.readUTF();
            long v = in.readLong();
            errorCounts.put(k, v);
        }
    }
}
