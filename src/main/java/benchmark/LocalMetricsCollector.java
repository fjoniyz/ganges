package benchmark;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.zeromq.ZMQ;

public class LocalMetricsCollector {

  private static LocalMetricsCollector instance;
  private static HashMap<String, HashMap<String, Long>> timestamps = new HashMap<>();
  private String remoteMetricsAddress;
  ZMQ.Context context = ZMQ.context(1);
  ZMQ.Socket socket;

  private LocalMetricsCollector() {
    String userDirectory = System.getProperty("user.dir");
    try (InputStream configStream = Files.newInputStream(
        Paths.get(userDirectory + "/src/main/resources/monitoring/local-metrics-collector" +
            ".properties"))) {
      Properties props = new Properties();
      props.load(configStream);
      remoteMetricsAddress = props.getProperty("remote-collector-address");
    } catch (IOException e) {
      e.printStackTrace();
    }

    connectToSocket();
  }

  private void connectToSocket() {
    socket = context.socket(ZMQ.PUB);
    socket.connect(remoteMetricsAddress);
  }

  public static LocalMetricsCollector getInstance()
      throws ExecutionException, InterruptedException {
    if (instance == null) {
      instance = new LocalMetricsCollector();
    }
    return instance;
  }

  public void setProducerTimestamps(JsonNode message) {
    String id = message.get("ae_session_id").textValue();
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
