package com.aitrace.job.analytics.latency;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class LatencyHistogramJob extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: LatencyHistogramJob <input> <output> [-Duse.combiner=true/false] [-Dmapreduce.job.reduces=N]");
            return -1;
        }

        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "Latency Histogram Job");
        job.setJarByClass(LatencyHistogramJob.class);

        job.setMapperClass(LatencyHistogramMapper.class);
        job.setReducerClass(LatencyHistogramReducer.class);

        boolean useCombiner = conf.getBoolean("use.combiner", true);
        if (useCombiner) {
            job.setCombinerClass(LatencyHistogramCombiner.class);
        }

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LatencyStatsWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        int numReducers = conf.getInt("mapreduce.job.reduces", 2);
        if (numReducers > 2) numReducers = 2; // at most 2 reducers
        job.setNumReduceTasks(numReducers);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new LatencyHistogramJob(), args));
    }
}
