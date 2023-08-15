package benchmark;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantLock;

public class LocalMetricsCollector {

  private static LocalMetricsCollector instance;
  private static HashMap<String, HashMap<String, Long>> timestamps = new HashMap<>();
  //private final AsynchronousSocketChannel clientSocket;
  private final String REMOTE_METRICS_ADDRESS = "localhost";
  private final int REMOTE_METRICS_PORT = 5000;
  private Future<Integer> lastSendingResult;
  private final ReentrantLock timestampsLock = new ReentrantLock();

  private LocalMetricsCollector() throws ExecutionException, InterruptedException, IOException {

  }

  public static LocalMetricsCollector getInstance()
      throws IOException, ExecutionException, InterruptedException {
    if (instance == null) {
      instance = new LocalMetricsCollector();
    }
    return instance;
  }

  public void setProducerTimestamps(JsonNode message) {
    String id = message.get("id").toString();
    Long producerTimestamp = Long.parseLong(message.get("producerTimestamp").toString());
    setTimestamp(id, "producer", producerTimestamp);
  }

  public void setAnonEntryTimestamps(String id, long timestamp) {
    setTimestamp(id, "anonEntry", timestamp);

  }

  public void setConsumerTimestamps(String id, long timestamp) {
    setTimestamp(id, "consumer", timestamp);
    sendCurrentResultsToRemote();
  }

  public void setPipeEntryTimestamps(String id, long timestamp) {
    setTimestamp(id, "pipeEntry", timestamp);
    sendCurrentResultsToRemote();
  }

  public void setPipeExitTimestamps(String id, long timestamp) {
    setTimestamp(id, "pipeExit", timestamp);
    sendCurrentResultsToRemote();
  }

  public void setAnonExitTimestamps(String id, long timestamp) {
    setTimestamp(id, "anonExit", timestamp);
  }

  private void setTimestamp(String id, String timestampName, Long timestamp) {
    timestampsLock.lock();
    if (!timestamps.containsKey(id)) {
      timestamps.put(id, new HashMap<>());
    }
    timestamps.get(id).put(timestampName, timestamp);
    timestampsLock.unlock();
  }

  private void sendCurrentResultsToRemote() {
    ObjectMapper mapper = new ObjectMapper();

    timestampsLock.lock();
    JsonNode jsonNode = mapper.valueToTree(timestamps);
    sendMessage(jsonNode);
    timestamps.clear();
    timestampsLock.unlock();
  }

  private void sendMessage(JsonNode message) {
    try {
      AsynchronousSocketChannel clientSocket = AsynchronousSocketChannel.open();
      Future<Void> future = clientSocket.connect(new InetSocketAddress(REMOTE_METRICS_ADDRESS,
          REMOTE_METRICS_PORT));
      future.get();
      if (lastSendingResult != null) {
        lastSendingResult.get();
      }
      System.out.println("Sending message to remote metrics collector: " + message.toString());
      ByteBuffer buffer = ByteBuffer.wrap(message.toString().getBytes(StandardCharsets.UTF_8));
      lastSendingResult =
          clientSocket.write(buffer);
      lastSendingResult.get();
    } catch (InterruptedException | ExecutionException | IOException e) {
      e.printStackTrace();
    }

  }
}
