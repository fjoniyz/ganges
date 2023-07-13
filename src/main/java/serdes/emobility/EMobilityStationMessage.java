package serdes.emobility;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import lombok.Data;
import serdes.AnonymizedMessage;

@JsonRootName("chargingStationMessage")
@Data
public class EMobilityStationMessage implements Serializable, AnonymizedMessage {
  private String id;
  private String timestamp;
  private String timeseriesId;
  private double evUsage;

  @JsonCreator
  public EMobilityStationMessage(@JsonProperty("id")String id, @JsonProperty("Timestamp")String timestamp, @JsonProperty("timeseries_id")String timeseriesId,
                                 @JsonProperty("Seconds_EnergyConsumption")double evUsage) {
    this.id = id;
    this.timestamp = timestamp;
    this.timeseriesId = timeseriesId;
    this.evUsage = evUsage;
  }

  public double[] getValuesListFromKeys(String[] keys) {
    List<Double> values = new ArrayList<>();
    for (String field : keys) {
      switch (field) {
        case "evUsage":
          values.add(getEvUsage());
          break;
        default:
          System.out.println("Invalid field in config file: " + field);
      }
    }
    return values.stream().mapToDouble(d -> d).toArray();
  }

  public void setEvUsage(double evUsage) {
    this.evUsage = evUsage;
  }
}
