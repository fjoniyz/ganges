package benchmark;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantLock;
import org.zeromq.ZMQ;

public class LocalMetricsCollector {

  private static LocalMetricsCollector instance;
  private static HashMap<String, HashMap<String, Long>> timestamps = new HashMap<>();
  private final String REMOTE_METRICS_ADDRESS = "tcp://*:12346";
  ZMQ.Context context = ZMQ.context(1);
  ZMQ.Socket socket;

  private LocalMetricsCollector() throws IOException {
    connectToSocket();
  }

  private void connectToSocket() {
    socket = context.socket(ZMQ.PUB);
    socket.connect(REMOTE_METRICS_ADDRESS);
  }

  public static LocalMetricsCollector getInstance()
      throws IOException, ExecutionException, InterruptedException {
    if (instance == null) {
      instance = new LocalMetricsCollector();
    }
    return instance;
  }

  public void setProducerTimestamps(JsonNode message) {
    String id = message.get("id").textValue();
    Long producerTimestamp = Long.parseLong(message.get("producer_timestamp").toString());
    setTimestamp(id, "producer", producerTimestamp);
  }

  public void setAnonEntryTimestamps(String id, long timestamp) {
    setTimestamp(id, "anonEntry", timestamp);

  }

  public void setConsumerTimestamps(String id, long timestamp) {
    setTimestamp(id, "consumer", timestamp);
  }

  public void setPipeEntryTimestamps(String id, long timestamp) {
    setTimestamp(id, "pipeEntry", timestamp);
  }

  public void setPipeExitTimestamps(String id, long timestamp) {
    setTimestamp(id, "pipeExit", timestamp);
  }

  public void setAnonExitTimestamps(String id, long timestamp) {
    setTimestamp(id, "anonExit", timestamp);
  }

  private void setTimestamp(String id, String timestampName, Long timestamp) {
//    timestampsLock.lock();
    if (!timestamps.containsKey(id)) {
      timestamps.put(id, new HashMap<>());
    }
    timestamps.get(id).put(timestampName, timestamp);
  }

  public void sendCurrentResultsToRemote() {
    if (timestamps.isEmpty()) {
      System.err.println("Timestamps are empty");
      return;
    }
    ObjectMapper mapper = new ObjectMapper();

    JsonNode jsonNode = mapper.valueToTree(timestamps);
    try {
      sendMessage(jsonNode);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    timestamps.clear();
  }

  private void sendMessage(JsonNode message) throws InterruptedException {
    boolean sent = socket.send(message.toString());
    System.out.println("Sent message to remote metrics collector: " + message + " sending result:"
            + " " + sent);
  }
}
