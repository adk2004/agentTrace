package com.aitrace.job.session;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SessionizationJob extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: SessionizationJob <input> <output> [-Dmapreduce.job.reduces=2]");
            return -1;
        }

        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "Sessionization Job");
        job.setJarByClass(SessionizationJob.class);

        // Mapper / Reducer
        job.setMapperClass(SessionMapper.class);
        job.setReducerClass(SessionReducer.class);

        // Key/Value types
        job.setMapOutputKeyClass(RunTimeKey.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        // Secondary sort plumbing
        job.setPartitionerClass(RunIdPartitioner.class);
        job.setSortComparatorClass(RunTimeKey.SortComparator.class);
        job.setGroupingComparatorClass(RunIdGroupingComparator.class);

        // Reducers: default 2, overridable with -Dmapreduce.job.reduces=N
        int numReducers = conf.getInt("mapreduce.job.reduces", 2);
        job.setNumReduceTasks(numReducers);

        // I/O formats
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Session rollup side output via MultipleOutputs
        MultipleOutputs.addNamedOutput(job, "session", TextOutputFormat.class, NullWritable.class, Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new SessionizationJob(), args));
    }
}
