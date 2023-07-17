package myapps;

import java.util.List;
import java.util.Map;

public interface AnonymizationAlgorithm {
    List<Map<String, Double>> anonymize(List<Map<String, Double>> X);
}
