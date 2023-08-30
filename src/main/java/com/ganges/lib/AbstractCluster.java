package com.ganges.lib;

import com.ganges.lib.castleguard.utils.Utils;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.Range;

public abstract class AbstractCluster {

  protected Map<String, Range<Float>> ranges;
  protected Map<String, Float> headerWeights;
  protected final Utils utils;

  public AbstractCluster(List<String> headers, Map<String, Float> headerWeights) {
    // Initialises the cluster
    this.ranges = new LinkedHashMap<>();
    headers.forEach(header -> this.ranges.put(header, Range.between(0f, 0f)));
    this.utils =  new Utils();
    this.headerWeights = headerWeights;
  }

  public Map<String, Float> getHeaderWeights() {
    return this.headerWeights;
  }

  public Map<String, Range<Float>> getRanges() {
    return this.ranges;
  }

  public void setRanges(HashMap<String, Range<Float>> value) {
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
  public float informationLoss(HashMap<String, Range<Float>> globalRanges) {
    float loss = 0f;

    // For each range, check if <item> would extend it
    Range<Integer> updated = null;
    for (Map.Entry<String, Range<Float>> header : this.ranges.entrySet()) {
      Range<Float> range = globalRanges.get(header.getKey());
      loss += this.utils.rangeInformationLoss(header.getValue(), range, this.headerWeights.get(header.getKey()));
    }
    return loss;
  }
}
