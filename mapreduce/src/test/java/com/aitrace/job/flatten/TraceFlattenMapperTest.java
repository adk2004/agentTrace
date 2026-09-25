package com.aitrace.job.flatten;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import static org.mockito.Mockito.*;
import static org.junit.Assert.*;
public class TraceFlattenMapperTest {
    private TraceFlattenMapper mapper;
    private Mapper.Context context;
    private Counter totalLines, validRuns, malformedLines, emptySteps, negativeLatency, stepsEmitted;
    private MultipleOutputs<NullWritable, Text> mos;
    @Before
    public void setup() throws Exception {
        mapper = new TraceFlattenMapper();
        context = mock(Mapper.Context.class);
        mos = mock(MultipleOutputs.class);
        totalLines = mock(Counter.class);
        validRuns = mock(Counter.class);
        malformedLines = mock(Counter.class);
        emptySteps = mock(Counter.class);
        negativeLatency = mock(Counter.class);
        stepsEmitted = mock(Counter.class);
        when(context.getCounter(TraceFlattenMapper.TraceQuality.TOTAL_LINES)).thenReturn(totalLines);
        when(context.getCounter(TraceFlattenMapper.TraceQuality.VALID_RUNS)).thenReturn(validRuns);
        when(context.getCounter(TraceFlattenMapper.TraceQuality.MALFORMED_LINES)).thenReturn(malformedLines);
        when(context.getCounter(TraceFlattenMapper.TraceQuality.EMPTY_STEPS)).thenReturn(emptySteps);
        when(context.getCounter(TraceFlattenMapper.TraceQuality.NEGATIVE_LATENCY_FIXED)).thenReturn(negativeLatency);
        when(context.getCounter(TraceFlattenMapper.TraceQuality.STEPS_EMITTED)).thenReturn(stepsEmitted);
        mapper.mos = mos;
        mapper.setup(context);
    }
    @Test
    public void testValidRow() throws Exception {
        String json = "{\"trace_id\":\"abc\",\"run_id\":\"run1\",\"steps\":[{\"latency_ms\":100,\"timestamp\":\"2026-01-03T19:48:42.000Z\"}]}";
        mapper.map(new LongWritable(1), new Text(json), context);
        verify(totalLines).increment(1);
        verify(validRuns).increment(1);
        verify(stepsEmitted).increment(1);
        ArgumentCaptor<Text> captor = ArgumentCaptor.forClass(Text.class);
        verify(context).write(eq(NullWritable.get()), captor.capture());
        String output = captor.getValue().toString();
        assertTrue(output.startsWith("run1\t\\N\tabc\t\\N\t\\N"));
        assertTrue(output.contains("1767469722000"));
        assertTrue(output.contains("2026-01-03"));
    }
    @Test
    public void testMalformedRow() throws Exception {
        String json = "{\"trace_id\":\"abc\",";
        mapper.map(new LongWritable(1), new Text(json), context);
        verify(totalLines).increment(1);
        verify(malformedLines).increment(1);
        verify(mos).write(eq("malformed"), eq(NullWritable.get()), any(Text.class), eq("_malformed/part"));
    }
}
