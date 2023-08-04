package com.ganges.lib;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface AnonymizationAlgorithm {
  List<AnonymizationItem> anonymizeItem(List<AnonymizationItem> X);

  public List<Map<String, Double>> anonymize(List<Map<String, Double>> X); // deprecated
}