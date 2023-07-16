package serdes.chargingstation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;
import java.io.Serializable;
import java.util.UUID;
import lombok.Data;
import serdes.AnonymizedMessage;

@JsonRootName("chargingStationMessage")
@Data
public class ChargingStationMessage implements Serializable, AnonymizedMessage {
  private UUID aeSessionId;
  private String buildingType;
  private float urbanisationLevel;
  private int numberLoadingStations;
  private int numberParkingSpaces;
  private long startTimeLoading;
  private long endTimeLoading;
  private int loadingTime;
  private float kwh;
  private int loadingPotential;

  @JsonCreator
  public ChargingStationMessage(@JsonProperty("ae_session_id")UUID aeSessionId,
                                @JsonProperty("building_type") String buildingType,
                                @JsonProperty("urbanisation_level") float urbanisationLevel,
                                @JsonProperty("number_loading_stations") int numberLoadingStations,
                                @JsonProperty("number_parking_spaces") int numberParkingSpaces,
                                @JsonProperty("start_time_loading")long startTimeLoading,
                                @JsonProperty("end_time_loading") long endTimeLoading,
                                @JsonProperty("loading_time") int loadingTime,
                                @JsonProperty("kwh") float kwh,
                                @JsonProperty("loading_potential")int loadingPotential) {
    this.aeSessionId = aeSessionId;
    this.buildingType = buildingType;
    this.urbanisationLevel = urbanisationLevel;
    this.numberLoadingStations = numberLoadingStations;
    this.numberParkingSpaces = numberParkingSpaces;
    this.startTimeLoading = startTimeLoading;
    this.endTimeLoading = endTimeLoading;
    this.loadingTime = loadingTime;
    this.kwh = kwh;
    this.loadingPotential = loadingPotential;
  }

  @Override
  public String getId() {
    return aeSessionId.toString();
  }

  @Override
  public Double[] getValuesListByKeys(String[] keys) {
    return new Double[0]; // TODO: Move function from pipe her
  }
}
