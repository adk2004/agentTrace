package com.aitrace.util;
import java.time.Instant;
import java.time.format.DateTimeParseException;
public class TimeUtil {
    public static Long parseIsoToEpochMillis(String isoUtc) {
        if (isoUtc == null || isoUtc.trim().isEmpty()) { return null; }
        try { return Instant.parse(isoUtc).toEpochMilli(); } catch (DateTimeParseException e) { return null; }
    }
}
