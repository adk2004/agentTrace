package com.aitrace.job.analytics.latency;

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

public class LatencyHistogramTest {

    @Test
    public void testBuckets() {
        assertEquals("<100", LatencyHistogramMapper.getBucket(50));
        assertEquals("<100", LatencyHistogramMapper.getBucket(99));
        assertEquals("100-250", LatencyHistogramMapper.getBucket(100));
        assertEquals("100-250", LatencyHistogramMapper.getBucket(249));
        assertEquals("250-500", LatencyHistogramMapper.getBucket(250));
        assertEquals("500-1000", LatencyHistogramMapper.getBucket(999));
        assertEquals("1000-2500", LatencyHistogramMapper.getBucket(1500));
        assertEquals("2500-5000", LatencyHistogramMapper.getBucket(3000));
        assertEquals("5000-10000", LatencyHistogramMapper.getBucket(7500));
        assertEquals(">=10000", LatencyHistogramMapper.getBucket(10000));
        assertEquals(">=10000", LatencyHistogramMapper.getBucket(25000));
    }

    @Test
    public void testMapper() throws Exception {
        LatencyHistogramMapper mapper = new LatencyHistogramMapper();
        Mapper.Context ctx = mock(Mapper.Context.class);
        Counter c = mock(Counter.class);
        when(ctx.getCounter(any())).thenReturn(c);

        String line = "run1\ts1\tt1\tagent\tcoding\tgpt-4o\tsuccess\tpro\tus\t1\t0\ttool_call\t1000\t2026-01-01\t350\t\\N\t\\N\t\\N\tweb_search\tok\t\\N\t0\t0";
        mapper.map(new LongWritable(1), new Text(line), ctx);

        ArgumentCaptor<Text> keyCap = ArgumentCaptor.forClass(Text.class);
        ArgumentCaptor<LatencyStatsWritable> valCap = ArgumentCaptor.forClass(LatencyStatsWritable.class);
        verify(ctx).write(keyCap.capture(), valCap.capture());

        assertEquals("gpt-4o|tool_call\t250-500", keyCap.getValue().toString());
        assertEquals(1, valCap.getValue().getCount());
        assertEquals(350, valCap.getValue().getMin());
        assertEquals(350, valCap.getValue().getMax());
        assertEquals(350, valCap.getValue().getSum());
    }

    @Test
    public void testCombinerAndReducer() throws Exception {
        LatencyStatsWritable w1 = new LatencyStatsWritable(1, 260, 260, 260);
        LatencyStatsWritable w2 = new LatencyStatsWritable(1, 400, 400, 400);

        LatencyHistogramCombiner combiner = new LatencyHistogramCombiner();
        Reducer.Context combCtx = mock(Reducer.Context.class);
        combiner.reduce(new Text("gpt-4o|tool_call\t250-500"), Arrays.asList(w1, w2), combCtx);

        ArgumentCaptor<LatencyStatsWritable> combCap = ArgumentCaptor.forClass(LatencyStatsWritable.class);
        verify(combCtx).write(eq(new Text("gpt-4o|tool_call\t250-500")), combCap.capture());
        LatencyStatsWritable combined = combCap.getValue();

        assertEquals(2, combined.getCount());
        assertEquals(260, combined.getMin());
        assertEquals(400, combined.getMax());
        assertEquals(660, combined.getSum());

        LatencyHistogramReducer reducer = new LatencyHistogramReducer();
        Reducer.Context redCtx = mock(Reducer.Context.class);
        Counter redCounter = mock(Counter.class);
        when(redCtx.getCounter(any())).thenReturn(redCounter);

        reducer.reduce(new Text("gpt-4o|tool_call\t250-500"), Arrays.asList(combined), redCtx);
        ArgumentCaptor<Text> outCap = ArgumentCaptor.forClass(Text.class);
        verify(redCtx).write(eq(NullWritable.get()), outCap.capture());

        String[] cols = outCap.getValue().toString().split("\t");
        assertEquals("gpt-4o|tool_call", cols[0]);
        assertEquals("250-500", cols[1]);
        assertEquals("2", cols[2]);       // count
        assertEquals("260", cols[3]);     // min
        assertEquals("400", cols[4]);     // max
        assertEquals("330.00", cols[5]);  // avg
        assertEquals("660", cols[6]);     // sum
    }
}
