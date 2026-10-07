package com.aitrace.job.analytics.token;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import java.util.Arrays;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class TokenStatsTest {

    @Test
    public void testMapperFilterAndBucketing() throws Exception {
        TokenStatsMapper mapper = new TokenStatsMapper();
        Mapper.Context ctx = mock(Mapper.Context.class);
        Counter c = mock(Counter.class);
        when(ctx.getCounter(any())).thenReturn(c);

        // step_index = 0 -> bucket 0-1
        String line1 = "run1\ts1\tt1\tagent\tcoding\tgpt-4o\tsuccess\tpro\tus\t1\t0\tllm_call\t1000\t2026-01-01\t200\t100\t50\tstop\t\\N\t\\N\t\\N\t0\t0";
        mapper.map(new LongWritable(1), new Text(line1), ctx);

        ArgumentCaptor<Text> keyCap = ArgumentCaptor.forClass(Text.class);
        ArgumentCaptor<TokenStatsWritable> valCap = ArgumentCaptor.forClass(TokenStatsWritable.class);
        verify(ctx).write(keyCap.capture(), valCap.capture());

        assertEquals("gpt-4o|coding|2026-01-01", keyCap.getValue().toString());
        assertEquals(1, valCap.getValue().getLlmCalls());
        assertEquals(100, valCap.getValue().getSumPrompt());
        assertEquals(50, valCap.getValue().getSumCompletion());
        assertEquals(150, valCap.getValue().getTokens0_1());
        assertEquals(0, valCap.getValue().getTokens2_4());
    }

    @Test
    public void testCombinerAndReducer() throws Exception {
        // step 0 (bucket 0-1)
        TokenStatsWritable w1 = new TokenStatsWritable(1, 100, 50, 0);
        // step 3 (bucket 2-4)
        TokenStatsWritable w2 = new TokenStatsWritable(1, 200, 100, 3);
        // step 12 (bucket 10+)
        TokenStatsWritable w3 = new TokenStatsWritable(1, 500, 250, 12);

        TokenStatsCombiner combiner = new TokenStatsCombiner();
        Reducer.Context combCtx = mock(Reducer.Context.class);
        combiner.reduce(new Text("gpt-4o|coding|2026-01-01"), Arrays.asList(w1, w2, w3), combCtx);

        ArgumentCaptor<TokenStatsWritable> combCap = ArgumentCaptor.forClass(TokenStatsWritable.class);
        verify(combCtx).write(eq(new Text("gpt-4o|coding|2026-01-01")), combCap.capture());
        TokenStatsWritable combined = combCap.getValue();

        assertEquals(3, combined.getLlmCalls());
        assertEquals(800, combined.getSumPrompt());
        assertEquals(400, combined.getSumCompletion());
        assertEquals(500, combined.getMaxPrompt());
        assertEquals(250, combined.getMaxCompletion());
        assertEquals(150, combined.getTokens0_1());
        assertEquals(300, combined.getTokens2_4());
        assertEquals(0, combined.getTokens5_9());
        assertEquals(750, combined.getTokens10Plus());

        // Reducer
        TokenStatsReducer reducer = new TokenStatsReducer();
        Reducer.Context redCtx = mock(Reducer.Context.class);
        Counter redCounter = mock(Counter.class);
        when(redCtx.getCounter(any())).thenReturn(redCounter);

        reducer.reduce(new Text("gpt-4o|coding|2026-01-01"), Arrays.asList(combined), redCtx);
        ArgumentCaptor<Text> outCap = ArgumentCaptor.forClass(Text.class);
        verify(redCtx).write(eq(NullWritable.get()), outCap.capture());

        String[] cols = outCap.getValue().toString().split("\t");
        assertEquals("gpt-4o|coding|2026-01-01", cols[0]);
        assertEquals("3", cols[1]);       // llm_calls
        assertEquals("800", cols[2]);     // sum_prompt
        assertEquals("400", cols[3]);     // sum_completion
        assertEquals("266.67", cols[4]);  // avg_prompt
        assertEquals("133.33", cols[5]);  // avg_completion
        assertEquals("500", cols[6]);     // max_prompt
        assertEquals("250", cols[7]);     // max_completion
        assertEquals("150", cols[8]);     // bucket 0-1
        assertEquals("300", cols[9]);     // bucket 2-4
        assertEquals("0", cols[10]);      // bucket 5-9
        assertEquals("750", cols[11]);    // bucket 10+
    }
}
