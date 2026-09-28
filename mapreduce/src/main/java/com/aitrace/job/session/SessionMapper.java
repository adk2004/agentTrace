package com.aitrace.job.session;
import com.aitrace.model.SpanEvent;
import com.aitrace.parser.TraceParser;
import com.aitrace.util.TsvUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Counter;
import java.io.IOException;
import java.util.Optional;
public class SessionMapper extends Mapper<LongWritable, Text, RunTimeKey, Text> {
    public enum SpanCounters { TOTAL_LINES, PARSED_SPANS, MALFORMED_SPANS }
    private TraceParser parser;
    private RunTimeKey outKey;
    private Text outValue;
    @Override
    protected void setup(Context context) {
        parser   = new TraceParser();
        outKey   = new RunTimeKey();
        outValue = new Text();
    }
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.getCounter(SpanCounters.TOTAL_LINES).increment(1);
        String line = value.toString().trim();
        if (line.isEmpty()) return;
        Optional<SpanEvent> opt = parser.parseSpanEvent(line);
        if (!opt.isPresent()) {
            context.getCounter(SpanCounters.MALFORMED_SPANS).increment(1);
            return;
        }
        SpanEvent s = opt.get();
        if (s.getRun_id() == null || s.getTimestamp() == null) { context.getCounter(SpanCounters.MALFORMED_SPANS).increment(1); return; }
        context.getCounter(SpanCounters.PARSED_SPANS).increment(1);
        long ts = 0L;
        try { ts = Long.parseLong(s.getTimestamp()); } catch (NumberFormatException ignored) {}
        outKey.set(s.getRun_id(), ts);
        // compact payload: span_id TAB session_id TAB agent_name TAB task_type TAB model TAB run_status
        //   TAB step_index TAB event_type TAB ts TAB latency_ms
        //   TAB llm_prompt TAB llm_completion TAB finish_reason
        //   TAB tool_name TAB tool_status TAB error_type TAB retry_count TAB args_size
        //   TAB user_tier TAB region TAB agent_version TAB has_neg_lat
        StringBuilder sb = new StringBuilder();
        sb.append(TsvUtil.escape(s.getSpan_id())).append('\t')
          .append(TsvUtil.escape(s.getSession_id())).append('\t')
          .append(TsvUtil.escape(s.getAgent_name())).append('\t')
          .append(TsvUtil.escape(s.getTask_type())).append('\t')
          .append(TsvUtil.escape(s.getModel())).append('\t')
          .append(TsvUtil.escape(s.getRun_status())).append('\t')
          .append(TsvUtil.escape(s.getStep_index())).append('\t')
          .append(TsvUtil.escape(s.getEvent_type())).append('\t')
          .append(ts).append('\t')
          .append(TsvUtil.escape(s.getLatency_ms())).append('\t')
          .append(TsvUtil.escape(s.getLlm_prompt_tokens())).append('\t')
          .append(TsvUtil.escape(s.getLlm_completion_tokens())).append('\t')
          .append(TsvUtil.escape(s.getLlm_finish_reason())).append('\t')
          .append(TsvUtil.escape(s.getTool_name())).append('\t')
          .append(TsvUtil.escape(s.getTool_status())).append('\t')
          .append(TsvUtil.escape(s.getTool_error_type())).append('\t')
          .append(TsvUtil.escape(s.getTool_retry_count())).append('\t')
          .append(TsvUtil.escape(s.getTool_args_size_bytes())).append('\t')
          .append(TsvUtil.escape(s.getUser_tier())).append('\t')
          .append(TsvUtil.escape(s.getRegion())).append('\t')
          .append(TsvUtil.escape(s.getAgent_version())).append('\t')
          .append(s.getHas_negative_latency() != null && s.getHas_negative_latency() ? "1" : "0");
        outValue.set(sb.toString());
        context.write(outKey, outValue);
    }
}
