package com.aitrace.job.session;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import java.util.Arrays;
import java.util.List;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class SessionReducerTest {
    private SessionReducer reducer;
    private Reducer.Context ctx;
    private MultipleOutputs<NullWritable, Text> mos;
    private Counter runsBuilt, orphan, dupe, longRuns;

    // Helper: build a flat payload string matching SessionMapper format
    // spanId, sessionId, agentName, taskType, model, runStatus, stepIndex, evType, ts, lat,
    // llmPrompt, llmCompl, finishReason, toolName, toolStatus, errorType, retryCount, argsSize,
    // userTier, region, agentVersion, hasNegLat
    private static String span(String spanId, String sessId, String evType, long ts, long lat,
                               int prompt, int compl, String toolName, String toolStatus, String errType, int retries) {
        return spanId + "\t" + sessId + "\tagent1\tcoding\tgpt-4o\tsuccess\t0\t" + evType + "\t" + ts + "\t" + lat
              + "\t" + prompt + "\t" + compl + "\tstop\t" + nn(toolName) + "\t" + nn(toolStatus) + "\t" + nn(errType) + "\t" + retries + "\t100"
              + "\tpro\tus-east-1\t1.0\t0";
    }

    private static String nn(String s) { return s == null ? "\\N" : s; }

    @Before
    public void setup() throws Exception {
        reducer = new SessionReducer();
        ctx = mock(Reducer.Context.class);
        mos = mock(MultipleOutputs.class);
        runsBuilt = mock(Counter.class);
        orphan = mock(Counter.class);
        dupe = mock(Counter.class);
        longRuns = mock(Counter.class);

        when(ctx.getCounter(SessionReducer.RunCounters.RUNS_BUILT)).thenReturn(runsBuilt);
        when(ctx.getCounter(SessionReducer.RunCounters.ORPHAN_EVENTS)).thenReturn(orphan);
        when(ctx.getCounter(SessionReducer.RunCounters.DUPLICATE_SPANS)).thenReturn(dupe);
        when(ctx.getCounter(SessionReducer.RunCounters.LONG_RUNS)).thenReturn(longRuns);

        reducer.mos = mos;
        reducer.setup(ctx);
    }

    @Test
    public void testNormalRun() throws Exception {
        List<Text> values = Arrays.asList(
            new Text(span("s1", "sess1", "llm_call", 1000L, 200, 100, 50, null, null, null, 0)),
            new Text(span("s2", "sess1", "tool_call", 2000L, 300, 0, 0, "web_search", "ok", null, 0)),
            new Text(span("s3", "sess1", "llm_call", 3000L, 400, 80, 40, null, null, null, 0))
        );

        reducer.reduce(new RunTimeKey("run1", 1000L), values, ctx);
        verify(runsBuilt).increment(1);

        ArgumentCaptor<Text> cap = ArgumentCaptor.forClass(Text.class);
        verify(ctx).write(eq(NullWritable.get()), cap.capture());

        String row = cap.getValue().toString();
        String[] cols = row.split("\t");

        assertEquals(22, cols.length);
        assertEquals("run1", cols[0]);          // run_id
        assertEquals("sess1", cols[1]);         // session_id
        assertEquals("agent1", cols[2]);        // agent_name
        assertEquals("coding", cols[3]);        // task_type
        assertEquals("gpt-4o", cols[4]);        // model
        assertEquals("1000", cols[5]);          // start_ts
        assertEquals("3000", cols[6]);          // end_ts
        assertEquals("2000", cols[7]);          // duration_ms (3000 - 1000)
        assertEquals("3", cols[8]);             // total_steps
        assertEquals("2", cols[9]);             // llm_calls
        assertEquals("1", cols[10]);            // tool_calls
        assertEquals("0", cols[11]);            // tool_failures
        assertEquals("0", cols[12]);            // tool_timeouts
        assertEquals("0", cols[13]);            // total_retries
        assertEquals("180", cols[14]);          // prompt_tokens_total (100+80)
        assertEquals("90", cols[15]);           // completion_tokens_total (50+40)
        assertEquals("900", cols[16]);          // total_latency_ms (200+300+400)
        assertEquals("400", cols[17]);          // max_step_latency_ms
        assertEquals("1", cols[18]);            // distinct_tools_used
        assertEquals("\\N", cols[19]);          // first_error_type
        assertEquals("success", cols[20]);      // final_status
        assertEquals("0", cols[21]);            // is_retry_loop = false
    }

    @Test
    public void testRetryLoop() throws Exception {
        // Same tool failed 3 consecutive times = retry loop
        List<Text> values = Arrays.asList(
            new Text(span("s1", "sess2", "tool_call", 1000L, 100, 0, 0, "code_exec", "failed", "RuntimeError", 1)),
            new Text(span("s2", "sess2", "tool_call", 2000L, 100, 0, 0, "code_exec", "failed", "RuntimeError", 1)),
            new Text(span("s3", "sess2", "tool_call", 3000L, 100, 0, 0, "code_exec", "failed", "RuntimeError", 1))
        );

        reducer.reduce(new RunTimeKey("run2", 1000L), values, ctx);
        ArgumentCaptor<Text> cap = ArgumentCaptor.forClass(Text.class);
        verify(ctx).write(eq(NullWritable.get()), cap.capture());

        String[] cols = cap.getValue().toString().split("\t");
        assertEquals("1", cols[21]);            // is_retry_loop = true
        assertEquals("3", cols[11]);            // tool_failures
        assertEquals("RuntimeError", cols[19]); // first_error_type
        assertEquals("3", cols[13]);            // total_retries
    }

    @Test
    public void testDuplicateSpanDedupAndOrphans() throws Exception {
        // Span s1 appears twice; also an orphan line (<22 fields)
        List<Text> values = Arrays.asList(
            new Text(span("s1", "sess3", "llm_call", 1000L, 200, 100, 50, null, null, null, 0)),
            new Text(span("s1", "sess3", "llm_call", 1000L, 200, 100, 50, null, null, null, 0)),
            new Text("short_invalid_line\twith_few_fields"),
            new Text(span("s2", "sess3", "llm_call", 2000L, 100, 50, 25, null, null, null, 0))
        );

        reducer.reduce(new RunTimeKey("run3", 1000L), values, ctx);
        verify(dupe).increment(1);
        verify(orphan).increment(1);

        ArgumentCaptor<Text> cap = ArgumentCaptor.forClass(Text.class);
        verify(ctx).write(eq(NullWritable.get()), cap.capture());

        String[] cols = cap.getValue().toString().split("\t");
        assertEquals("2", cols[8]);   // total_steps = 2 (not 4)
    }

    @Test
    public void testSessionRollupCleanup() throws Exception {
        List<Text> r1 = Arrays.asList(
            new Text(span("s1", "sess_agg", "llm_call", 1000L, 200, 100, 50, null, null, null, 0))
        );
        List<Text> r2 = Arrays.asList(
            new Text(span("s2", "sess_agg", "llm_call", 2000L, 300, 200, 100, null, null, null, 0))
        );

        reducer.reduce(new RunTimeKey("run_a", 1000L), r1, ctx);
        reducer.reduce(new RunTimeKey("run_b", 2000L), r2, ctx);
        reducer.cleanup(ctx);

        ArgumentCaptor<Text> sessionCap = ArgumentCaptor.forClass(Text.class);
        verify(mos).write(eq("session"), eq(NullWritable.get()), sessionCap.capture(), eq("session_rollup/part"));

        String sessionRow = sessionCap.getValue().toString();
        String[] sCols = sessionRow.split("\t");
        assertEquals("sess_agg", sCols[0]); // session_id
        assertEquals("2", sCols[1]);        // runs_in_session
        assertEquals("450", sCols[2]);      // total_tokens (150 + 300)
    }
}
