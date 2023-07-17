package benchmark;

import com.opencsv.CSVWriter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MetricsCollector {

  private static MetricsCollector instance;

  private static Set<String> allIds = new LinkedHashSet<>();
  private static HashMap<String, Long> producerTimestamps = new HashMap<>();
  private static HashMap<String, Long> pipeEntryTimestamps = new HashMap<>();
  private static HashMap<String, Long> anonEntryTimestamps = new HashMap<>();
  private static HashMap<String, Long> anonExitTimestamps = new HashMap<>();
  private static HashMap<String, Long> pipeExitTimestamps = new HashMap<>();
  private static HashMap<String, Long> consumerTimestamps = new HashMap<>();


  private String fileName;

  private MetricsCollector() {
  }

  public static MetricsCollector getInstance() {
    if (instance == null) {
      instance = new MetricsCollector();
    }
    return instance;
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

  public void saveMetricsToCSV() throws IOException {

    File file = new File(fileName);

    try {
      // create FileWriter object with file as parameter
      FileWriter outputfile = new FileWriter(file);

      // create CSVWriter object filewriter object as parameter
      CSVWriter writer = new CSVWriter(outputfile);

      // adding header to csv
      String[] header = { "ID", "ProducerTimestamp", "EntryPipeTimestamp",
          "EntryAnonymizationTimestamp", "ExitAnonymizationTimestamp", "ExitPipeTimestamp",
          "ConsumerTimestamp"};
      writer.writeNext(header);

      for (String id : allIds) {
        List<String> dataList = new ArrayList<>();

        String producerTimestamp = producerTimestamps.containsKey(id)
            ? producerTimestamps.get(id).toString() : "";
        String pipeEntryTimestamp = pipeEntryTimestamps.containsKey(id)
            ? pipeEntryTimestamps.get(id).toString() : "";
        String pipeExitTimestamp = pipeExitTimestamps.containsKey(id)
            ? pipeExitTimestamps.get(id).toString() : "";
        String anonEntryTimestamp = anonEntryTimestamps.containsKey(id)
            ? anonEntryTimestamps.get(id).toString() : "";
        String anonExitTimestamp = anonExitTimestamps.containsKey(id)
            ? anonExitTimestamps.get(id).toString() : "";
        String consumerTimestamp = consumerTimestamps.containsKey(id)
            ? consumerTimestamps.get(id).toString() : "";

        String[] data = {id, producerTimestamp, pipeEntryTimestamp, anonEntryTimestamp,
            anonExitTimestamp, pipeExitTimestamp, consumerTimestamp };
        writer.writeNext(data);
      }
      // closing writer connection
      writer.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static Long getProducerTimestamps(String id) {
    return producerTimestamps.get(id);
  }

  public static Long getConsumerTimestamps(String id) {
    return consumerTimestamps.get(id);
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

  public static void setProducerTimestamps(String id, long timestamp) {
    allIds.add(id);
    producerTimestamps.put(id, timestamp);
  }

  public static void setAnonEntryTimestamps(String id, long timestamp) {
    allIds.add(id);
    anonEntryTimestamps.put(id, timestamp);
  }

  public static void setConsumerTimestamps(String id, long timestamp) {
    allIds.add(id);
    consumerTimestamps.put(id, timestamp);
  }

  public static void setPipeEntryTimestamps(String id, long timestamp) {
    allIds.add(id);
    pipeEntryTimestamps.put(id, timestamp);
  }

  public static void setPipeExitTimestamps(String id, long timestamp) {
    allIds.add(id);
    pipeExitTimestamps.put(id, timestamp);
  }

  public static void setAnonExitTimestamps(String id, long timestamp) {
    allIds.add(id);
    anonExitTimestamps.put(id, timestamp);
  }

  public void setFileName(String fileName) {
    this.fileName = fileName;
  }
}
