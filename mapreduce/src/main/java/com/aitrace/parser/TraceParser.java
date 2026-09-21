package com.aitrace.parser;
import com.aitrace.model.RunRecord;
import com.aitrace.model.SpanEvent;
import com.aitrace.model.StepRecord;
import com.aitrace.util.TimeUtil;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParseException;
import java.util.Optional;
public class TraceParser {
    private static final Gson GSON = new GsonBuilder().create();
    public Optional<RunRecord> parseRun(String jsonLine) {
        if (jsonLine == null || jsonLine.trim().isEmpty()) { return Optional.empty(); }
        try {
            RunRecord run = GSON.fromJson(jsonLine, RunRecord.class);
            if (run == null || run.getTrace_id() == null) { return Optional.empty(); }
            run.setHas_negative_latency(false);
            if (run.getStart_time() != null) {
                Long epoch = TimeUtil.parseIsoToEpochMillis(run.getStart_time());
                if (epoch != null) run.setStart_time(epoch.toString());
            }
            if (run.getEnd_time() != null) {
                Long epoch = TimeUtil.parseIsoToEpochMillis(run.getEnd_time());
                if (epoch != null) run.setEnd_time(epoch.toString());
            }
            if (run.getSteps() != null) {
                for (StepRecord step : run.getSteps()) {
                    if (step.getTimestamp() != null) {
                        Long epoch = TimeUtil.parseIsoToEpochMillis(step.getTimestamp());
                        if (epoch != null) step.setTimestamp(epoch.toString());
                    }
                    if (step.getLatency_ms() != null && step.getLatency_ms() < 0) {
                        step.setLatency_ms(0L);
                        run.setHas_negative_latency(true);
                    }
                }
            }
            return Optional.of(run);
        } catch (JsonParseException | NumberFormatException | ClassCastException e) {
            return Optional.empty();
        }
    }
    public Optional<SpanEvent> parseSpanEvent(String jsonLine) {
        if (jsonLine == null || jsonLine.trim().isEmpty()) { return Optional.empty(); }
        try {
            SpanEvent span = GSON.fromJson(jsonLine, SpanEvent.class);
            if (span == null || span.getTrace_id() == null || span.getSpan_id() == null) { return Optional.empty(); }
            span.setHas_negative_latency(false);
            if (span.getTimestamp() != null) {
                Long epoch = TimeUtil.parseIsoToEpochMillis(span.getTimestamp());
                if (epoch != null) span.setTimestamp(epoch.toString());
            }
            if (span.getLatency_ms() != null && span.getLatency_ms() < 0) {
                span.setLatency_ms(0L);
                span.setHas_negative_latency(true);
            }
            return Optional.of(span);
        } catch (JsonParseException | NumberFormatException | ClassCastException e) {
            return Optional.empty();
        }
    }
}
