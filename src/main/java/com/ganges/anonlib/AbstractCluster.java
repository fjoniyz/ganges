package com.ganges.anonlib;

import com.ganges.anonlib.castleguard.utils.Utils;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import org.apache.commons.lang3.Range;

public abstract class AbstractCluster {

  protected final Utils utils;
  @Getter
  protected Map<String, Range<Double>> ranges;
  @Getter
  protected Map<String, Double> headerWeights;

  public AbstractCluster(List<String> headers, Map<String, Double> headerWeights) {
    // Initialises the cluster
    this.ranges = new LinkedHashMap<>();
    headers.forEach(header -> this.ranges.put(header, Range.between(0.0, 0.0)));
    this.utils = new Utils();
    this.headerWeights = headerWeights;
  }

  public void setRanges(HashMap<String, Range<Double>> value) {
    this.ranges = value;
  }

  public void insert(AbstractItem item) {
    throw new UnsupportedOperationException("Not implemented");
  }

  public void remove(AbstractItem item) {
    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Calculates the information loss of this cluster.
   *
   * @param globalRanges The globally known ranges for each attribute
   * @return The current information loss of the cluster
   */
  public double informationLoss(HashMap<String, Range<Double>> globalRanges) {
    float loss = 0f;

    // For each range, check if <item> would extend it
    for (Map.Entry<String, Range<Double>> header : this.ranges.entrySet()) {
      Range<Double> range = globalRanges.get(header.getKey());
      loss += this.utils.rangeInformationLoss(header.getValue(), range,
          this.headerWeights.get(header.getKey()));
    }
    return loss;
  }
}
