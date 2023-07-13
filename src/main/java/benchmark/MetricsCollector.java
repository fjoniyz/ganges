package benchmark;

import java.util.HashMap;
import java.util.Map;

public class MetricsCollector {

  private static MetricsCollector instance;
  private static HashMap<String, Long> pipeEntryTimestamps = new HashMap<>();
  private static HashMap<String, Long> pipeExitTimestamps = new HashMap<>();
  private static HashMap<String, Long> anonEntryTimestamps = new HashMap<>();
  private static HashMap<String, Long> anonExitTimestamps = new HashMap<>();

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

  public static void printMetrics() {
    long averagePipeTime = 0;
    int anonymized = 0;
    for (Map.Entry<String, Long> entry : pipeEntryTimestamps.entrySet()) {
      if (pipeExitTimestamps.containsKey(entry.getKey())) {
        long duration = pipeExitTimestamps.get(entry.getKey()) - entry.getValue();
        averagePipeTime += duration;
        anonymized++;
      }
    }

    long averageAnonTime = 0;
    for (Map.Entry<String, Long> entry : anonEntryTimestamps.entrySet()) {
      if (anonExitTimestamps.containsKey(entry.getKey())) {
        long duration = pipeExitTimestamps.get(entry.getKey()) - entry.getValue();
        averagePipeTime += duration;
        anonymized++;
      }
    }

    averagePipeTime /= anonymized;
    averageAnonTime /= anonEntryTimestamps.size();

    System.out.println("Average pipe time: " + averagePipeTime + "ms");
    System.out.println("Average anonymization time: " + averageAnonTime + "ms");
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
