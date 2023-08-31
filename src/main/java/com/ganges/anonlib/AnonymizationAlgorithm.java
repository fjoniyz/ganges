package com.ganges.anonlib;

import java.util.List;

public interface AnonymizationAlgorithm {
  List<AnonymizationItem> anonymize(List<AnonymizationItem> X);
}
