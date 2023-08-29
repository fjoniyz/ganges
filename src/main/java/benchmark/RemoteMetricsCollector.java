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
  public static final String[] REQUIRED_TIMESTAMPS = new String[] {};
  private static String fileName;
  private static int port;
  public static final String[] OPTIONAL_TIMESTAMPS = new String[] {"producer", "pipeEntry",
      "anonEntry", "anonExit", "pipeExit", "Consumer"};
  private static Integer maxCheckCount = 50;
  private static final HashMap<String, HashMap<String, Long>> timestamps = new HashMap<>();
  private static final HashMap<String, Integer> recordCheckCount = new HashMap<>();
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static CSVWriter writer;

  public static void main(String[] args)
      throws ExecutionException, InterruptedException, IOException {

    String userDirectory = System.getProperty("user.dir");
    try (InputStream configStream = Files.newInputStream(
        Paths.get(userDirectory + "/src/main/resources/monitoring/remote-metrics-collector" +
            ".properties"))) {
      Properties props = new Properties();
      props.load(configStream);
      port = Integer.parseInt(props.getProperty("collector-socket-port"));
      maxCheckCount = Integer.valueOf(props.getProperty("max-check-count"));
      fileName = props.getProperty("csv-file-name");
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
      if (!timestamps.containsKey(id)) {
        timestamps.put(id, new HashMap<>());
      }

      // Put all timestamps for this id
      for (Iterator<String> idNodeTimestamp = idNode.fieldNames(); idNodeTimestamp.hasNext(); ) {
        String timestampName = idNodeTimestamp.next();
        long timestamp = Long.parseLong(idNode.get(timestampName).toString());

        timestamps.get(id).put(timestampName, timestamp);
      }
    }

    saveMetricsToCsv();
  }

  private static void saveMetricsToCsv() throws IOException {
    System.out.println("Saving metrics to CSV...");
    List<String> outputIds = new ArrayList<>();
    for (String id : timestamps.keySet()) {
      HashMap<String, Long> recordTimestamps = timestamps.get(id);
      if (Arrays.stream(REQUIRED_TIMESTAMPS)
          .allMatch(recordTimestamps::containsKey)) {
        if (Arrays.stream(OPTIONAL_TIMESTAMPS)
            .allMatch(recordTimestamps::containsKey)
            || (recordCheckCount.containsKey(id) && recordCheckCount.get(id) >= maxCheckCount)) {

          String[] data = {
            id,
            recordTimestamps.get("producer").toString(),
            recordTimestamps.get("pipeEntry").toString(),
            recordTimestamps.get("anonEntry").toString(),
            recordTimestamps.get("anonExit").toString(),
            recordTimestamps.get("pipeExit").toString(),
              !recordTimestamps.containsKey("consumer") ? "" :
                  recordTimestamps.get("consumer").toString()
          };
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
      timestamps.remove(id);
      recordCheckCount.remove(id);
    });
  }
}
