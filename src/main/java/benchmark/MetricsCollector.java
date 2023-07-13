package benchmark;

import java.util.HashMap;

public class MetricsCollector {

    private static MetricsCollector instance;
    private static HashMap<String, Long> pipeEntryTimestamps;
    private static HashMap<String, Long> pipeExitTimestamps;
    private static HashMap<String, Long> anonEntryTimestamps;
    private static HashMap<String, Long> anonExitTimestamps;

    private MetricsCollector() {
    }

    public static MetricsCollector getInstance() {
        if (instance == null) {
            instance = new MetricsCollector();
        }
        return instance;
    }

    public static long getPipeEntryTimestamp(String id) {
        return pipeEntryTimestamps.get(id);
    }

    public static long getPipeExitTimestamp(String id) {
        return pipeExitTimestamps.get(id);
    }

    public static long getAnonEntryTimestamp(String id) {
        return anonEntryTimestamps.get(id);
    }

    public static long getAnonExitTimestamp(String id) {
        return anonExitTimestamps.get(id);
    }

    public static long getAnonDuration(String id) {
        return anonExitTimestamps.get(id) - anonEntryTimestamps.get(id);
    }

    public static long getExitTimestamps(String id) {
        return pipeExitTimestamps.get(id) - pipeEntryTimestamps.get(id);
    }

    public static void setAnonEntryTimestamps(String id, long timestamp) {
        anonEntryTimestamps.put(id, timestamp);
    }

    public static void setPipeEntryTimestamps(String id, long timestamp) {
        pipeEntryTimestamps.put(id, timestamp);
    }

    public static void setPipeExitTimestamps(String id, long timestamp) {
        pipeExitTimestamps.put(id, timestamp);
    }

    public static void setAnonExitTimestamps(String id, long timestamp) {
        anonExitTimestamps.put(id, timestamp);
    }
}
