package com.ganges.lib;

import java.util.List;
import java.util.Map;

public interface AnonymizationAlgorithm {
  List<AnonymizationItem> anonymizeItem(List<AnonymizationItem> X);

  List<Map<String, Double>> anonymize(List<Map<String, Double>> X); // deprecated
}