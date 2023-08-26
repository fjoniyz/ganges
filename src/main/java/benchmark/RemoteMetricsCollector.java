package benchmark;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.CSVWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.zeromq.ZMQ;


public class RemoteMetricsCollector {

  public static final String[] CSV_HEADERS = new String[] {"ID", "Producer", "EntryPipe",
      "EntryAnonymization", "ExitAnonymization", "ExitPipe",
      "Consumer"};
  public static final String[] REQUIRED_TIMESTAMPS = new String[] {"producer", "pipeEntry",
      "anonEntry", "anonExit", "pipeExit"};
  private static final String FILE_NAME = "timestamps.csv";
  private static final int PORT = 12346;
  public static final String[] OPTIONAL_TIMESTAMPS = new String[] {"Consumer"};
  private static final Integer MAX_CHECK_COUNT = 50;
  private static HashMap<String, HashMap<String, Long>> timestamps = new HashMap<>();
  private static HashMap<String, Integer> recordCheckCount = new HashMap<>();
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static CSVWriter writer;

  public static void main(String[] args)
      throws ExecutionException, InterruptedException, IOException {

    File outputFile = new File(FILE_NAME);
    FileWriter fileWriter = new FileWriter(outputFile);
    // create CSVWriter object filewriter object as parameter
    writer = new CSVWriter(fileWriter);
    // adding header to csv
    writer.writeNext(CSV_HEADERS);
    writer.flush();

    System.out.println("Connecting to the socket with port " + PORT + "...");
    // Socket to talk to clients
    ZMQ.Context ctx = ZMQ.context(1);
    ZMQ.Socket socket = ctx.socket(ZMQ.SUB);
    socket.bind("tcp://127.0.0.1:" + PORT);
    socket.subscribe("".getBytes());

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
    List<String> outputIds = new ArrayList<>();
    for (String id : timestamps.keySet()) {
      HashMap<String, Long> recordTimestamps = timestamps.get(id);
      if (Arrays.stream(REQUIRED_TIMESTAMPS)
          .allMatch(recordTimestamps::containsKey)) {
        if (Arrays.stream(OPTIONAL_TIMESTAMPS)
            .allMatch(recordTimestamps::containsKey)
            || (recordCheckCount.containsKey(id) && recordCheckCount.get(id) >= MAX_CHECK_COUNT)) {

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
