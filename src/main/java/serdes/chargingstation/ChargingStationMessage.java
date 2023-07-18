package serdes.chargingstation;
import java.io.Serializable;
import java.util.Date;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;
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
    private String startTimeLoading;
    private String endTimeLoading;
    private int loadingTime;
    private float kwh;
    private int loadingPotential;

    @JsonCreator
    public ChargingStationMessage(@JsonProperty("ae_session_id")UUID ae_session_id, @JsonProperty("building_type") String building_type,
                                  @JsonProperty("urbanisation_level") float urbanisation_level, @JsonProperty("number_loading_stations") int number_loading_stations,
                                  @JsonProperty("number_parking_spaces") int number_parking_spaces, @JsonProperty("start_time_loading")String start_time_loading,
                                  @JsonProperty("end_time_loading") String end_time_loading, @JsonProperty("loading_time") int loading_time,
                                  @JsonProperty("kwh") float kwh, @JsonProperty("loading_potential")int loading_potential){
        this.aeSessionId = ae_session_id;
        this.buildingType = building_type;
        this.urbanisationLevel = urbanisation_level;
        this.numberLoadingStations = number_loading_stations;
        this.numberParkingSpaces = number_parking_spaces;
        this.startTimeLoading = start_time_loading;
        this.endTimeLoading = end_time_loading;
        this.loadingTime = loading_time;
        this.kwh = kwh;
        this.loadingPotential = loading_potential;
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
