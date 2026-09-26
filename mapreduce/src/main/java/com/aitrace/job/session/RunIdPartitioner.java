package com.aitrace.job.session;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
public class RunIdPartitioner extends Partitioner<RunTimeKey, Text> {
    @Override
    public int getPartition(RunTimeKey key, Text value, int numPartitions) {
        return (key.getRunId().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
