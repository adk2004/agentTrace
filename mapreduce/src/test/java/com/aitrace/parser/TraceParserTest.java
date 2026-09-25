package com.aitrace.parser;
import com.aitrace.model.RunRecord;
import com.aitrace.model.SpanEvent;
import org.junit.Before;
import org.junit.Test;
import java.util.Optional;
import static org.junit.Assert.*;
public class TraceParserTest {
    private TraceParser parser;
    @Before
    public void setUp() { parser = new TraceParser(); }
    @Test
    public void testValidRun() {
        String json = "{\"trace_id\":\"abc\",\"start_time\":\"2026-01-03T19:48:41.192Z\",\"steps\":[{\"latency_ms\":100,\"timestamp\":\"2026-01-03T19:48:42.000Z\"}]}";
        Optional<RunRecord> run = parser.parseRun(json);
        assertTrue(run.isPresent());
        assertEquals("abc", run.get().getTrace_id());
        assertEquals("1767469721192", run.get().getStart_time());
        assertFalse(run.get().getHas_negative_latency());
        assertEquals(100L, run.get().getSteps().get(0).getLatency_ms().longValue());
        assertEquals("1767469722000", run.get().getSteps().get(0).getTimestamp());
    }
    @Test
    public void testTruncated() {
        String json = "{\"trace_id\":\"abc\",\"steps\":[{\"latency_ms\":100";
        assertFalse(parser.parseRun(json).isPresent());
    }
    @Test
    public void testEmpty() {
        assertFalse(parser.parseRun("").isPresent());
        assertFalse(parser.parseRun("   ").isPresent());
    }
    @Test
    public void testWrongType() {
        String json = "{\"trace_id\":\"abc\",\"steps\":\"bad\"}";
        assertFalse(parser.parseRun(json).isPresent());
    }
    @Test
    public void testMissingToolBlock() {
        String json = "{\"trace_id\":\"abc\",\"steps\":[{\"step_index\":0}]}";
        Optional<RunRecord> run = parser.parseRun(json);
        assertTrue(run.isPresent());
        assertNull(run.get().getSteps().get(0).getTool());
    }
    @Test
    public void testNullSteps() {
        String json = "{\"trace_id\":\"abc\",\"steps\":null}";
        Optional<RunRecord> run = parser.parseRun(json);
        assertTrue(run.isPresent());
        assertNull(run.get().getSteps());
    }
    @Test
    public void testNegativeLatency() {
        String json = "{\"trace_id\":\"abc\",\"steps\":[{\"latency_ms\":-50}]}";
        Optional<RunRecord> run = parser.parseRun(json);
        assertTrue(run.isPresent());
        assertTrue(run.get().getHas_negative_latency());
        assertEquals(0L, run.get().getSteps().get(0).getLatency_ms().longValue());
    }
}
