package com.aitrace.job.session;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RunTimeKey implements WritableComparable<RunTimeKey> {
    private String runId;
    private long tsMilli;

    public RunTimeKey() {}

    public RunTimeKey(String runId, long tsMilli) {
        this.runId = runId;
        this.tsMilli = tsMilli;
    }

    public String getRunId() {
        return runId;
    }

    public long getTsMilli() {
        return tsMilli;
    }

    public void set(String runId, long tsMilli) {
        this.runId = runId;
        this.tsMilli = tsMilli;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(runId != null ? runId : "");
        out.writeLong(tsMilli);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        runId = in.readUTF();
        tsMilli = in.readLong();
    }

    @Override
    public int compareTo(RunTimeKey o) {
        int c = this.runId.compareTo(o.runId);
        return c != 0 ? c : Long.compare(this.tsMilli, o.tsMilli);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof RunTimeKey)) return false;
        RunTimeKey o = (RunTimeKey) obj;
        return runId != null && runId.equals(o.runId) && tsMilli == o.tsMilli;
    }

    @Override
    public int hashCode() {
        return runId != null ? runId.hashCode() : 0;
    }

    @Override
    public String toString() {
        return runId + "@" + tsMilli;
    }

    public static class SortComparator extends WritableComparator {
        public SortComparator() {
            super(RunTimeKey.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            RunTimeKey k1 = (RunTimeKey) a;
            RunTimeKey k2 = (RunTimeKey) b;
            return k1.compareTo(k2);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            try {
                int len1 = ((b1[s1] & 0xff) << 8) | (b1[s1 + 1] & 0xff);
                int len2 = ((b2[s2] & 0xff) << 8) | (b2[s2 + 1] & 0xff);
                int c = WritableComparator.compareBytes(b1, s1 + 2, len1, b2, s2 + 2, len2);
                if (c != 0) return c;
                long ts1 = readLong(b1, s1 + 2 + len1);
                long ts2 = readLong(b2, s2 + 2 + len2);
                return Long.compare(ts1, ts2);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    static {
        WritableComparator.define(RunTimeKey.class, new SortComparator());
    }
}
