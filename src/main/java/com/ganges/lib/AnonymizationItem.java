package com.ganges.lib;

import java.util.Map;
import java.util.Objects;

public class AnonymizationItem {
  private final String id;
  private Map<String, Double> values;
  private Map<String, Float> headerWeights;

  private final Map<String, String> nonAnonymizedValues;

  public AnonymizationItem(String id, Map<String, Double> values, Map<String, String> nonAnonymizedValues) {
    this.id = id;
    this.values = values;
    this.nonAnonymizedValues = nonAnonymizedValues;
  }

  public AnonymizationItem(String id, Map<String, Double> values, Map<String, String> nonAnonymizedValues,
                           Map<String, Float> headerWeights) {
    this.id = id;
    this.values = values;
    this.nonAnonymizedValues = nonAnonymizedValues;
    this.headerWeights = headerWeights;
  }

  public Map<String, Float> getHeaderWeights() {
      return headerWeights;
  }

  public Map<String, Double> getValues() {
    return values;
  }

  public Map<String, String> getNonAnonymizedValues() {
    return nonAnonymizedValues;
  }

  public String getId() {
    return id;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    AnonymizationItem item = (AnonymizationItem) o;
    return Objects.equals(id, item.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id);
  }
}
