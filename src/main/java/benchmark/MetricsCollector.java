package benchmark;

import com.opencsv.CSVWriter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
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

  public static Long getPipeEntryTimestamp(String id) {
    return pipeEntryTimestamps.get(id);
  }

  public static Long getPipeExitTimestamp(String id) {
    return pipeExitTimestamps.get(id);
  }

  public static Long getAnonEntryTimestamp(String id) {
    return anonEntryTimestamps.get(id);
  }

  public static Long getAnonExitTimestamp(String id) {
    return anonExitTimestamps.get(id);
  }

  public static Long getAnonDuration(String id) {
    return anonExitTimestamps.get(id) - anonEntryTimestamps.get(id);
  }

  public static Long getExitTimestamps(String id) {
    return pipeExitTimestamps.get(id);
  }

  public static void printMetrics() {
    long averagePipeTime = 0;
    //int anonymized = 0;
    for (Map.Entry<String, Long> entry : pipeEntryTimestamps.entrySet()) {
      if (pipeExitTimestamps.containsKey(entry.getKey())) {
        long duration = pipeExitTimestamps.get(entry.getKey()) - entry.getValue();
        averagePipeTime += duration;
        //anonymized++;
      }
    }

    long averageAnonTime = 0;
    for (Map.Entry<String, Long> entry : anonEntryTimestamps.entrySet()) {
      if (anonExitTimestamps.containsKey(entry.getKey())) {
        long duration = anonExitTimestamps.get(entry.getKey()) - entry.getValue();
        averageAnonTime += duration;
        //anonymized++;
      }
    }

    averagePipeTime /= pipeEntryTimestamps.size();
    averageAnonTime /= anonEntryTimestamps.size();

    System.out.println("Average pipe time: " + averagePipeTime + "ms");
    System.out.println("Average anonymization time: " + averageAnonTime + "ms");
  }

  public static void metricsToCsv() throws IOException {

    File file = new File("results.csv");

    try {
      // create FileWriter object with file as parameter
      FileWriter outputfile = new FileWriter(file);

      // create CSVWriter object filewriter object as parameter
      CSVWriter writer = new CSVWriter(outputfile);

      // adding header to csv
      String[] header = { "ID", "EntryPipe", "EntryAnonymization", "ExitAnonymization", "ExitPipe"};
      writer.writeNext(header);

      for (Map.Entry<String, Long> entry : pipeEntryTimestamps.entrySet()) {
        String id = entry.getKey();
        String[] data = {id, entry.getValue().toString(),getAnonEntryTimestamp(id).toString(), getAnonExitTimestamp(id).toString(), getExitTimestamps(id).toString() };
        writer.writeNext(data);
      }
      // closing writer connection
      writer.close();
    }
    catch (IOException e) {
      e.printStackTrace();
    }

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
