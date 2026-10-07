package com.aitrace.job.analytics.tool;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import java.util.Arrays;
import java.util.List;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class ToolStatsTest {

    @Test
    public void testMapperFiltersNonToolCalls() throws Exception {
        ToolStatsMapper mapper = new ToolStatsMapper();
        Mapper.Context ctx = mock(Mapper.Context.class);
        Counter c = mock(Counter.class);
        when(ctx.getCounter(any())).thenReturn(c);

        // step_type = llm_call -> should be skipped
        String llmLine = "run1\ts1\tt1\tagent\tcoding\tgpt4\tsuccess\tpro\tus\t1\t0\tllm_call\t1000\t2026-01-01\t200\t50\t25\tstop\t\\N\t\\N\t\\N\t0\t0";
        mapper.map(new LongWritable(1), new Text(llmLine), ctx);
        verify(ctx, never()).write(any(), any());

        // step_type = tool_call -> emitted
        String toolLine = "run1\ts1\tt1\tagent\tcoding\tgpt4\tsuccess\tpro\tus\t1\t1\ttool_call\t2000\t2026-01-01\t500\t\\N\t\\N\t\\N\tweb_search\tok\t\\N\t0\t100";
        mapper.map(new LongWritable(2), new Text(toolLine), ctx);
        ArgumentCaptor<Text> keyCap = ArgumentCaptor.forClass(Text.class);
        ArgumentCaptor<ToolStatsWritable> valCap = ArgumentCaptor.forClass(ToolStatsWritable.class);
        verify(ctx).write(keyCap.capture(), valCap.capture());

        assertEquals("web_search|coding", keyCap.getValue().toString());
        assertEquals(1, valCap.getValue().getCalls());
        assertEquals(1, valCap.getValue().getOk());
        assertEquals(500, valCap.getValue().getTotalLatencyMs());
    }

    @Test
    public void testCombinerAndReducer() throws Exception {
        ToolStatsWritable w1 = new ToolStatsWritable(1, 1, 0, 0, 200, 0, null);
        ToolStatsWritable w2 = new ToolStatsWritable(1, 0, 1, 0, 400, 2, "rate_limit");
        ToolStatsWritable w3 = new ToolStatsWritable(1, 0, 0, 1, 600, 1, "timeout");

        ToolStatsCombiner combiner = new ToolStatsCombiner();
        Reducer.Context combCtx = mock(Reducer.Context.class);
        combiner.reduce(new Text("code_exec|coding"), Arrays.asList(w1, w2, w3), combCtx);

        ArgumentCaptor<ToolStatsWritable> combCap = ArgumentCaptor.forClass(ToolStatsWritable.class);
        verify(combCtx).write(eq(new Text("code_exec|coding")), combCap.capture());
        ToolStatsWritable combined = combCap.getValue();
        assertEquals(3, combined.getCalls());
        assertEquals(1, combined.getOk());
        assertEquals(1, combined.getErrors());
        assertEquals(1, combined.getTimeouts());
        assertEquals(1200, combined.getTotalLatencyMs());
        assertEquals(3, combined.getTotalRetries());

        // Now Reducer
        ToolStatsReducer reducer = new ToolStatsReducer();
        Reducer.Context redCtx = mock(Reducer.Context.class);
        Counter redCounter = mock(Counter.class);
        when(redCtx.getCounter(any())).thenReturn(redCounter);

        reducer.reduce(new Text("code_exec|coding"), Arrays.asList(combined), redCtx);
        ArgumentCaptor<Text> outCap = ArgumentCaptor.forClass(Text.class);
        verify(redCtx).write(eq(NullWritable.get()), outCap.capture());

        String[] cols = outCap.getValue().toString().split("\t");
        assertEquals("code_exec|coding", cols[0]);
        assertEquals("3", cols[1]);    // calls
        assertEquals("1", cols[2]);    // ok
        assertEquals("1", cols[3]);    // errors
        assertEquals("1", cols[4]);    // timeouts
        assertEquals("0.6667", cols[5]); // failure_rate (2/3)
        assertEquals("400.00", cols[6]); // avg_latency
        assertEquals("1.00", cols[7]);   // avg_retries
        assertTrue(cols[8].contains("rate_limit:1"));
        assertTrue(cols[8].contains("timeout:1"));
    }
}
