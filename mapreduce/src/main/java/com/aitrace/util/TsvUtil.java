package com.aitrace.util;
public class TsvUtil {
    public static String escape(String value) {
        if (value == null) { return "\\N"; }
        return value.replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n").replace("\r", "\\r");
    }
    public static String escape(Number value) {
        if (value == null) { return "\\N"; }
        return value.toString();
    }
}
