package myapps;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface AnonymizationAlgorithm {
    List<AnonymizationItem> anonymize(List<AnonymizationItem> X);
}
