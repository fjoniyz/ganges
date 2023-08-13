package com.ganges.lib;

import java.util.List;

public interface AnonymizationAlgorithm {
    List<AnonymizationItem> anonymize(List<AnonymizationItem> X);
}
