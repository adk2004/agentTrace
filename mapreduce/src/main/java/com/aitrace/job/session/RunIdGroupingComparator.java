package com.aitrace.job.session;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class RunIdGroupingComparator extends WritableComparator {
    public RunIdGroupingComparator() {
        super(RunTimeKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        RunTimeKey k1 = (RunTimeKey) a;
        RunTimeKey k2 = (RunTimeKey) b;
        return k1.getRunId().compareTo(k2.getRunId());
    }

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        try {
            // Read only the run_id (UTF-8 in Hadoop DataOutput.writeUTF: 2-byte length prefix + bytes)
            int len1 = ((b1[s1] & 0xff) << 8) | (b1[s1 + 1] & 0xff);
            int len2 = ((b2[s2] & 0xff) << 8) | (b2[s2 + 1] & 0xff);
            return WritableComparator.compareBytes(b1, s1 + 2, len1, b2, s2 + 2, len2);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
