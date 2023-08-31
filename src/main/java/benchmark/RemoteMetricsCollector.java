package benchmark;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.CSVWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.zeromq.ZMQ;


public class RemoteMetricsCollector {

  public static final String[] CSV_HEADERS = new String[] {"ID", "Producer", "EntryPipe",
      "EntryAnonymization", "ExitAnonymization", "ExitPipe",
      "Consumer"};
  private static String fileName;
  private static int port;
  public static String[] requiredTimestamps;
  public static String[] optionalTimestamps;
  private static Integer maxCheckCount = 50;
  private static final HashMap<String, HashMap<String, Long>> metrics = new HashMap<>();
  private static final HashMap<String, Integer> recordCheckCount = new HashMap<>();
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static CSVWriter writer;
  private static final Properties properties = new Properties();

  public static void main(String[] args)
      throws ExecutionException, InterruptedException, IOException {

    String userDirectory = System.getProperty("user.dir");
    try (InputStream configStream = Files.newInputStream(
        Paths.get(userDirectory + "/src/main/resources/monitoring/remote-metrics-collector"
            + ".properties"))) {
      properties.load(configStream);
      port = Integer.parseInt(properties.getProperty("collector-socket-port"));
      maxCheckCount = Integer.valueOf(properties.getProperty("max-check-count"));
      fileName = properties.getProperty("csv-file-name");

      requiredTimestamps = properties.getProperty("required-timestamps").split(",");
      optionalTimestamps = properties.getProperty("optional-timestamps").split(",");

      System.out.println("Required metrics: " + Arrays.toString(requiredTimestamps));
      System.out.println("Optional metrics: " + Arrays.toString(optionalTimestamps));
    } catch (IOException e) {
      e.printStackTrace();
    }


    File outputFile = new File(fileName);
    FileWriter fileWriter = new FileWriter(outputFile);
    // create CSVWriter object filewriter object as parameter
    writer = new CSVWriter(fileWriter);
    // adding header to csv
    writer.writeNext(CSV_HEADERS);
    writer.flush();

    // Socket to talk to clients
    ZMQ.Context ctx = ZMQ.context(1);
    ZMQ.Socket socket = ctx.socket(ZMQ.SUB);
    String socketAddress = "tcp://*:" + port;
    socket.bind(socketAddress);
    socket.subscribe("".getBytes());
    System.out.println("Connecting to the socket: " + socketAddress + " ...");

    while (true) {
      String message = socket.recvStr();
      processMessage(message);
    }

  }

  private static void processMessage(String message)
      throws ExecutionException, InterruptedException, IOException {
    JsonNode receivedNode = objectMapper.readTree(message);
    System.out.println("Received: " + message);
    if (!message.isEmpty()) {
      processTimestampsJson(receivedNode);
    }
  }

  private static void processTimestampsJson(JsonNode rootNode) throws IOException {
    // Iterate through all entries
    for (Iterator<String> node = rootNode.fieldNames(); node.hasNext(); ) {
      String id = node.next();
      JsonNode idNode = rootNode.get(id);

      // Create a hashmap if not exists
      if (!metrics.containsKey(id)) {
        metrics.put(id, new HashMap<>());
      }

      // Put all timestamps for this id
      for (Iterator<String> idNodeTimestamp = idNode.fieldNames(); idNodeTimestamp.hasNext(); ) {
        String timestampName = idNodeTimestamp.next();
        long timestamp = Long.parseLong(idNode.get(timestampName).toString());

        metrics.get(id).put(timestampName, timestamp);
      }
    }

    saveMetricsToCsv();
  }

  private static void saveMetricsToCsv() {
    System.out.println("Saving metrics to CSV...");
    List<String> outputIds = new ArrayList<>();
    for (String id : metrics.keySet()) {
      HashMap<String, Long> recordTimestamps = metrics.get(id);
      if (Arrays.stream(requiredTimestamps)
          .allMatch(recordTimestamps::containsKey)) {
        if (Arrays.stream(optionalTimestamps)
            .allMatch(recordTimestamps::containsKey)
            || (recordCheckCount.containsKey(id) && recordCheckCount.get(id) >= maxCheckCount)) {
          List<String> dataList = new ArrayList<>(List.of(id));
          for (String timestampName : CSV_HEADERS) {
            if (recordTimestamps.containsKey(timestampName)) {
              dataList.add(recordTimestamps.get(timestampName).toString());
            } else {
              dataList.add("");
            }
          }
          String[] data = new String[dataList.size()];
          dataList.toArray(data);

          try {
            System.out.println("WRITING");
            writer.writeNext(data);
            writer.flush();
          } catch (IOException e) {
            e.printStackTrace();
          }

          outputIds.add(id);
        } else {
          if (recordCheckCount.containsKey(id)) {
            recordCheckCount.put(id, recordCheckCount.get(id) + 1);
          } else {
            recordCheckCount.put(id, 1);
          }
        }
      }
    }
    outputIds.forEach(id -> {
      metrics.remove(id);
      recordCheckCount.remove(id);
    });
  }
}
