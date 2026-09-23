package com.aitrace.job.flatten;
import com.aitrace.model.RunRecord;
import com.aitrace.model.StepRecord;
import com.aitrace.parser.TraceParser;
import com.aitrace.util.TsvUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Optional;
public class TraceFlattenMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    public enum TraceQuality { TOTAL_LINES, VALID_RUNS, MALFORMED_LINES, EMPTY_STEPS, NEGATIVE_LATENCY_FIXED, STEPS_EMITTED }
    private TraceParser parser;
    private Text outValue;
    MultipleOutputs<NullWritable, Text> mos;
    private int malformedCount = 0;
    private static final int MAX_MALFORMED_SAMPLES = 100;
    private static final DateTimeFormatter DT_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneId.of("UTC"));
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        parser = new TraceParser();
        outValue = new Text();
        if (mos == null) mos = new MultipleOutputs<>(context);
    }
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.getCounter(TraceQuality.TOTAL_LINES).increment(1);
        String line = value.toString();
        Optional<RunRecord> runOpt = parser.parseRun(line);
        if (!runOpt.isPresent()) {
            context.getCounter(TraceQuality.MALFORMED_LINES).increment(1);
            if (malformedCount < MAX_MALFORMED_SAMPLES) {
                mos.write("malformed", NullWritable.get(), value, "_malformed/part");
                malformedCount++;
            }
            return;
        }
        RunRecord run = runOpt.get();
        context.getCounter(TraceQuality.VALID_RUNS).increment(1);
        if (run.getHas_negative_latency() != null && run.getHas_negative_latency()) {
            context.getCounter(TraceQuality.NEGATIVE_LATENCY_FIXED).increment(1);
        }
        if (run.getSteps() == null || run.getSteps().isEmpty()) {
            context.getCounter(TraceQuality.EMPTY_STEPS).increment(1);
            return;
        }
        String userTier = run.getMetadata() != null ? run.getMetadata().getUser_tier() : null;
        String region = run.getMetadata() != null ? run.getMetadata().getRegion() : null;
        String agentVersion = run.getMetadata() != null ? run.getMetadata().getAgent_version() : null;
        for (StepRecord step : run.getSteps()) {
            String dt = "\\N";
            if (step.getTimestamp() != null) {
                try {
                    long epoch = Long.parseLong(step.getTimestamp());
                    dt = DT_FORMATTER.format(Instant.ofEpochMilli(epoch));
                } catch (NumberFormatException ignored) {}
            }
            Integer promptTokens = step.getLlm() != null ? step.getLlm().getPrompt_tokens() : null;
            Integer completionTokens = step.getLlm() != null ? step.getLlm().getCompletion_tokens() : null;
            String finishReason = step.getLlm() != null ? step.getLlm().getFinish_reason() : null;
            String toolName = step.getTool() != null ? step.getTool().getName() : null;
            String toolStatus = step.getTool() != null ? step.getTool().getStatus() : null;
            String errorType = step.getTool() != null ? step.getTool().getError_type() : null;
            Integer retryCount = step.getTool() != null ? step.getTool().getRetry_count() : null;
            Integer argsSizeBytes = step.getTool() != null ? step.getTool().getArgs_size_bytes() : null;
            StringBuilder sb = new StringBuilder();
            sb.append(TsvUtil.escape(run.getRun_id())).append("\t")
              .append(TsvUtil.escape(run.getSession_id())).append("\t")
              .append(TsvUtil.escape(run.getTrace_id())).append("\t")
              .append(TsvUtil.escape(run.getAgent_name())).append("\t")
              .append(TsvUtil.escape(run.getTask_type())).append("\t")
              .append(TsvUtil.escape(run.getModel())).append("\t")
              .append(TsvUtil.escape(run.getStatus())).append("\t")
              .append(TsvUtil.escape(userTier)).append("\t")
              .append(TsvUtil.escape(region)).append("\t")
              .append(TsvUtil.escape(agentVersion)).append("\t")
              .append(TsvUtil.escape(step.getStep_index())).append("\t")
              .append(TsvUtil.escape(step.getType())).append("\t")
              .append(TsvUtil.escape(step.getTimestamp())).append("\t")
              .append(dt).append("\t")
              .append(TsvUtil.escape(step.getLatency_ms())).append("\t")
              .append(TsvUtil.escape(promptTokens)).append("\t")
              .append(TsvUtil.escape(completionTokens)).append("\t")
              .append(TsvUtil.escape(finishReason)).append("\t")
              .append(TsvUtil.escape(toolName)).append("\t")
              .append(TsvUtil.escape(toolStatus)).append("\t")
              .append(TsvUtil.escape(errorType)).append("\t")
              .append(TsvUtil.escape(retryCount)).append("\t")
              .append(TsvUtil.escape(argsSizeBytes));
            outValue.set(sb.toString());
            context.write(NullWritable.get(), outValue);
            context.getCounter(TraceQuality.STEPS_EMITTED).increment(1);
        }
    }
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if (mos != null) { mos.close(); }
    }
}
