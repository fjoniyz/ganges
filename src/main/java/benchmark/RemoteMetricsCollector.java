package benchmark;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.CSVWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

public class RemoteMetricsCollector {

  //private static Set<String> allIds = new LinkedHashSet<>();
  private static HashMap<String, HashMap<String, Long>> timestamps = new HashMap<>();
  private static AsynchronousServerSocketChannel serverSocket;
  //private static AsynchronousSocketChannel socketChannel;
  private static String fileName = "timestamps.csv";
  private static final int PORT = 5000;

  private static ObjectMapper objectMapper = new ObjectMapper();

  public static void main(String[] args)
      throws ExecutionException, InterruptedException, IOException {
    connectSocket();
    while (true) {
      Future<AsynchronousSocketChannel> acceptResult = serverSocket.accept();
      AsynchronousSocketChannel socketChannel = acceptResult.get();
      metricsReaderLoop(socketChannel);
    }
  }

  private static void connectSocket() {
    try {
      serverSocket =
          AsynchronousServerSocketChannel.open();
      serverSocket.bind(new InetSocketAddress("localhost", PORT));
      System.out.println("Opened server socket on port " + PORT);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static void metricsReaderLoop(AsynchronousSocketChannel socketChannel)
      throws ExecutionException, InterruptedException, IOException {

    ByteBuffer buffer = ByteBuffer.allocate(1024);
    Future<Integer> readResult = socketChannel.read(buffer);
    readResult.get();
    buffer.flip();
    String receivedValue = new String(buffer.array(), StandardCharsets.UTF_8).replaceAll("\0", "");
    JsonNode receivedNode = objectMapper.readTree(receivedValue);

    System.out.println("Received: " + receivedValue);
    processMessage(receivedNode);
    socketChannel.close();
  }

  private static void processMessage(JsonNode rootNode) throws IOException {
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

    saveMetricsToCSV();
  }

  private static void saveMetricsToCSV() throws IOException {

    File file = new File(fileName);

    try {
      // create FileWriter object with file as parameter
      FileWriter outputfile = new FileWriter(file);

      // create CSVWriter object filewriter object as parameter
      CSVWriter writer = new CSVWriter(outputfile);

      // adding header to csv
      String[] header = { "ID", "Producer", "EntryPipe",
          "EntryAnonymization", "ExitAnonymization", "ExitPipe",
          "Consumer"};
      writer.writeNext(header);

      for (String id : timestamps.keySet()) {
        List<String> dataList = new ArrayList<>();

        String producerTimestamp = timestamps.containsKey(id)
            && timestamps.get(id).containsKey("producer")
            ? timestamps.get(id).get("producer").toString() : "";
        String pipeEntryTimestamp = timestamps.containsKey(id)
            && timestamps.get(id).containsKey("pipeEntry")
            ? timestamps.get(id).get("pipeEntry").toString() : "";
        String pipeExitTimestamp = timestamps.containsKey(id)
            && timestamps.get(id).containsKey("pipeExit")
            ? timestamps.get(id).get("pipeExit").toString() : "";
        String anonEntryTimestamp = timestamps.containsKey(id)
            && timestamps.get(id).containsKey("anonEntry")
            ? timestamps.get(id).get("anonEntry").toString() : "";
        String anonExitTimestamp = timestamps.containsKey(id)
            && timestamps.get(id).containsKey("anonExit")
            ? timestamps.get(id).get("anonExit").toString() : "";
        String consumerTimestamp = timestamps.containsKey(id)
            && timestamps.get(id).containsKey("consumer")
            ? timestamps.get(id).get("consumer").toString() : "";

        String[] data = {id, producerTimestamp, pipeEntryTimestamp, anonEntryTimestamp,
            anonExitTimestamp, pipeExitTimestamp, consumerTimestamp };
        writer.writeNext(data);

        boolean allPresent = !producerTimestamp.isEmpty() && !pipeEntryTimestamp.isEmpty()
            && !pipeExitTimestamp.isEmpty() && !anonEntryTimestamp.isEmpty()
            && !anonExitTimestamp.isEmpty() && !consumerTimestamp.isEmpty();
        if (allPresent) {
          timestamps.remove(id);
        }
      }
      // closing writer connection
      writer.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
