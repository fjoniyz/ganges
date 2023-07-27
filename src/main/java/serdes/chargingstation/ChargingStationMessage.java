package serdes.chargingstation;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
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
    private float startTimeLoading;
    private long endTimeLoading;
    private int loadingTime;
    private float kwh;
    private float loadingPotential;

    @JsonCreator
    public ChargingStationMessage(@JsonProperty("ae_session_id")UUID ae_session_id, @JsonProperty("building_type") String building_type,
                                  @JsonProperty("urbanisation_level") float urbanisation_level, @JsonProperty("number_loading_stations") int number_loading_stations,
                                  @JsonProperty("number_parking_spaces") int number_parking_spaces, @JsonProperty("start_time_loading")float start_time_loading,
                                  @JsonProperty("end_time_loading") long end_time_loading, @JsonProperty("loading_time") int loading_time,
                                  @JsonProperty("kwh") float kwh, @JsonProperty("loading_potential")float loading_potential){
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
    public String getTimestamp() {
        return null;
    }

    @Override
    public double[] getValuesListFromKeys(String[] keys) {
        List<Double> values = new ArrayList<>();
        for (String field : keys) {
            switch (field) {
                case "urbanisation_level":
                    System.out.println("urb level: " + getUrbanisationLevel());
                    values.add((double) getUrbanisationLevel());
                    break;
                case "number_loading_stations":
                    System.out.println("number load stat: " + getNumberLoadingStations());
                    values.add((double) getNumberLoadingStations());
                    break;
                case "number_parking_spaces":
                    System.out.println("parking spaces" + getNumberParkingSpaces());
                    values.add((double) getNumberParkingSpaces());
                    break;
                case "start_time_loading":
                    System.out.println("loading time start" + getStartTimeLoading());
                    values.add((double) getStartTimeLoading());
                    break;
                case "end_time_loading":
                    System.out.println("load time end" + getEndTimeLoading());
                    values.add((double) getEndTimeLoading());
                    break;
                case "loading_time":
                    System.out.println("loading time" + getLoadingTime());
                    values.add((double) getLoadingTime());
                    break;
                case "kwh":
                    System.out.println("kwh: " + getKwh());
                    values.add((double) getKwh());
                    break;
                case "loading_potential":
                    System.out.println("load potential " + getLoadingPotential());
                    values.add((double) getLoadingPotential());
                    break;
                default:
                    System.out.println("Invalid field in config file: " + field);
            }
        }
        return values.stream().mapToDouble(d -> d).toArray();
    }

    @Override
    public Double[] getValuesListByKeys(String[] keys) {
        return new Double[0]; // TODO: Move function from pipe her
    }
}
