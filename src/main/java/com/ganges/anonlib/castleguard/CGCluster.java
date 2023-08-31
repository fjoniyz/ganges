package com.ganges.anonlib.castleguard;

import com.ganges.anonlib.AbstractCluster;
import com.ganges.anonlib.castleguard.utils.Utils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.commons.lang3.Range;

public class CGCluster extends AbstractCluster {
  private final List<CGItem> contents;
  private Set<Double> diversity;
  private Map<String, Double> sampleValues;
  //private final Map<String, Double> headerWeights;

  public CGCluster(List<String> headers, Map<String, Double> headerWeights) {
    super(headers, headerWeights);
    // Initialises the cluster
    this.diversity = new HashSet<>();
    this.sampleValues = new HashMap<>();
    this.contents = new ArrayList<>();
  }

  public List<CGItem> getContents() {
    return this.contents;
  }

  public void addContents(List<CGItem> value) {
    //this.contents = value;
    for (CGItem item : value) {
      this.insert(item);
    }
  }

  /***
   * Check number of individuals in Cluster
   ***/
  public int getKSize() {
    Set<Double> pids = new HashSet<>();

    for (CGItem item : this.contents) {
      pids.add(item.getPid());
    }
    return pids.size();
  }


  public int getSize() {
    return this.contents.size();
  }

  public int getDiversitySize() {
    return this.diversity.size();
  }


  public Map<String, Double> getSampleValues() {
    return this.sampleValues;
  }

  public void setSampleValues(Map<String, Double> value) {
    this.sampleValues = value;
  }

  public Set<Double> getDiversity() {
    return this.diversity;
  }

  public void setDiversity(Set<Double> value) {
    this.diversity = value;
  }

  /**
   * Inserts a tuple into the cluster
   *
   * @param element The element to insert into the cluster
   */
  public void insert(CGItem element) {
    // checks for an empty cluster
    boolean firstElem = this.contents.isEmpty();
    this.contents.add(element);

    // Check whether the item is already in a cluster
    if (element.getCluster() != null) {
      // If it is, remove it so that we do not reach an invalid state
      element.getCluster().remove(element);
    }
    // Add sensitive attribute value to the diversity of cluster
    this.diversity.add((element).getSensitiveAttr());
    element.setCluster(this);

    // in case of an empty Cluster the Ranges are set to the items values
    if (firstElem) {
      for (Map.Entry<String, Range<Double>> header : this.ranges.entrySet()) {
        header.setValue(
            Range.between(
                element.getData().get(header.getKey()), element.getData().get(header.getKey())));
      }
      // Otherwise we search for the Minimum /Maximum
    } else {
      for (Map.Entry<String, Range<Double>> header : this.ranges.entrySet()) {
        header.setValue(
            Range.between(
                Math.min(header.getValue().getMinimum(), element.getData().get(header.getKey())),
                Math.max(header.getValue().getMaximum(), element.getData().get(header.getKey()))));
      }
    }
  }

  /**
   * Removes a tuple from the cluster
   *
   * @param element: The element to remove from the cluster
   */
  public void remove(CGItem element) {
    this.contents.remove(element);

    element.setCluster(null);

    boolean containsSensitiveAttr = false;
    for (CGItem e : this.contents) {
      if (Objects.equals(e.getSensitiveAttr(), element.getSensitiveAttr())) {
        containsSensitiveAttr = true;
        break;
      }
    }
    if (!containsSensitiveAttr) {
      this.diversity.remove(element.getSensitiveAttr());
    }
  }

  /**
   * Generalises a tuple based on the ranges for this cluster
   *
   * @param item: The tuple to be generalised
   * @return: A generalised version of the tuple based on the ranges for this cluster
   */
  // Note: Return value with only Item -> In Cluster.py return value (gen_tuple, item)
  CGItem generalise(CGItem item) {
    for (Map.Entry<String, Range<Double>> header : this.ranges.entrySet()) {
      if (!this.sampleValues.containsKey(header.getKey())) {
        this.sampleValues.put(
            header.getKey(), Utils.randomChoice(this.contents).getData().get(header.getKey()));
      }
      item.removeHeaders("pid");
      Double min = header.getValue().getMinimum();
      Double max = header.getValue().getMaximum();
      item.getData().put("min" + header.getKey(), min);
      item.getData().put("spc" + header.getKey(), this.sampleValues.get(header.getKey()));
      item.getData().put("max" + header.getKey(), max);

      item.getData().put(header.getKey(), (min + max) / 2); // Mean of the range

      item.addHeaders("min" + header.getKey());
      item.addHeaders("spc" + header.getKey());
      item.addHeaders("max" + header.getKey());

      // item.removeHeaders("pid");
    }
    return item;
  }

  /**
   * Calculates the enlargement value for adding <item> into this cluster
   *
   * @param item:         The tuple to calculate enlargement based on
   * @param globalRanges: The globally known ranges for each attribute
   * @return The information loss if we added item into this cluster
   */
  public Double tupleEnlargement(CGItem item, HashMap<String, Range<Double>> globalRanges) {
    Double given = this.informationLossGivenT(item, globalRanges);
    Double current = this.informationLoss(globalRanges);
    return (given - current) / this.ranges.size();
  }

  public Double clusterEnlargement(CGCluster CGCluster, HashMap<String, Range<Double>> globalRanges) {
    Double given = this.informationLossGivenC(CGCluster, globalRanges);
    Double current = this.informationLoss(globalRanges);
    return (given - current) / this.ranges.size();
  }

  /**
   * Calculates the information loss upon adding <item> into this cluster
   *
   * @param item:          The tuple to calculate information loss based on
   * @param global_ranges: The globally known ranges for each attribute
   * @return: The information loss given that we insert item into this cluster
   */
  Double informationLossGivenT(CGItem item, HashMap<String, Range<Double>> global_ranges) {
    Double loss = 0.0;
    if (this.contents.isEmpty()) {
      return 0.0;
    }
    // For each range, check if <item> would extend it
    Range<Double> updated;
    for (Map.Entry<String, Range<Double>> header : this.ranges.entrySet()) {
      Range<Double> global_range = global_ranges.get(header.getKey());
      updated =
          Range.between(
              (Math.min(header.getValue().getMinimum(), item.getData().get(header.getKey()))),
              Math.max(header.getValue().getMaximum(), item.getData().get(header.getKey())));
      loss += this.utils.rangeInformationLoss(updated, global_range, this.headerWeights.get(header.getKey()));
    }
    return loss;
  }

  /**
   * Calculates the information loss upon merging <cluster> into this cluster
   *
   * @param CGCluster:       The cluster to calculate information loss based on
   * @param global_ranges: The globally known ranges for each attribute
   * @return: The information loss given that we merge cluster with this cluster
   */
  public Double informationLossGivenC(CGCluster CGCluster, HashMap<String, Range<Double>> global_ranges) {
    Double loss = 0.0;
    if (this.contents.isEmpty()) {
      return CGCluster.informationLoss(global_ranges);
    } else if (CGCluster.contents.isEmpty()) {
      return this.informationLoss(global_ranges);
    }
    // For each range, check if <item> would extend it
    Range<Double> updated;
    for (Map.Entry<String, Range<Double>> header : this.ranges.entrySet()) {
      Range<Double> global_range = global_ranges.get(header.getKey());
      updated =
          Range.between(
              Math.min(
                  header.getValue().getMinimum(), CGCluster.ranges.get(header.getKey()).getMinimum()),
              Math.max(
                  header.getValue().getMaximum(),
                  CGCluster.ranges.get(header.getKey()).getMaximum()));
      loss += this.utils.rangeInformationLoss(updated, global_range, this.headerWeights.get(header.getKey()));
    }
    return loss;
  }


  /**
   * Calculates the distance from this tuple to another
   *
   * @param other: The tuple to calculate the distance to
   * @return: The tuple to calculate the distance to
   */
  public Double distance(CGItem other) {
    Double total_distance = 0.0;
    for (Map.Entry<String, Range<Double>> header : this.ranges.entrySet()) {
      total_distance +=
          Math.abs(
              other.getData().get(header.getKey()) - this.utils.rangeDifference(header.getValue()));
    }
    return total_distance;
  }

  /**
   * Checks whether a tuple is within all the ranges of the cluster, e.g. would cause no information
   * loss on being entered.
   *
   * @param item: The tuple to perform bounds checking on
   * @return: Whether the tuple is within the bounds of the cluster
   */
  public boolean withinBounds(CGItem item) {
    for (Map.Entry<String, Range<Double>> header : this.ranges.entrySet()) {
      if (!header.getValue().contains(item.getData().get(header.getKey()))) {
        return false;
      }
    }
    return true;
  }

}
