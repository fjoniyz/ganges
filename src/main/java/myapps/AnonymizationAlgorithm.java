package myapps;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface AnonymizationAlgorithm {
    Optional<List<Map<String, Double>>> anonymize(List<Map<String, Double>> X);
}
