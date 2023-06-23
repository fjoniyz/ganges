package customSerdes;
import java.io.Serializable;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;
import lombok.Data;

@JsonRootName("chargingStationMessage")
@Data
public class ChargingStationMessage implements Serializable {
    private UUID ae_session_id;
    private String building_type;
    private float urbanisation_level;
    private int number_loading_stations;
    private int number_parking_spaces;
    private long start_time_loading;
    private long end_time_loading;
    private int loading_time;
    private float kwh;
    private int loading_potential;

    @JsonCreator
    public ChargingStationMessage(@JsonProperty("ae_session_id")UUID ae_session_id, @JsonProperty("building_type") String building_type,
                                  @JsonProperty("urbanisation_level") float urbanisation_level, @JsonProperty("number_loading_stations") int number_loading_stations,
                                  @JsonProperty("number_parking_spaces") int number_parking_spaces, @JsonProperty("start_time_loading")long start_time_loading,
                                  @JsonProperty("end_time_loading") long end_time_loading, @JsonProperty("loading_time") int loading_time,
                                  @JsonProperty("kwh") float kwh, @JsonProperty("loading_potential")int loading_potential){
        this.ae_session_id = ae_session_id;
        this.building_type = building_type;
        this.urbanisation_level = urbanisation_level;
        this.number_loading_stations = number_loading_stations;
        this.number_parking_spaces = number_parking_spaces;
        this.start_time_loading = start_time_loading;
        this.end_time_loading = end_time_loading;
        this.loading_time = loading_time;
        this.kwh = kwh;
        this.loading_potential = loading_potential;
    }
}
