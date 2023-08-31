package com.ganges.anonlib.doca;

import com.ganges.anonlib.AbstractCluster;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.Range;

public class DocaCluster extends AbstractCluster {

  private final List<DocaItem> contents;


  public DocaCluster(List<String> headers, Map<String, Double> headerWeights) {
    // Initialises the cluster
    super(headers, headerWeights);
    this.contents = new ArrayList<>();

  }

  public List<DocaItem> getContents() {
    return this.contents;
  }

  public int getSize() {
    return this.contents.size();
  }


  /**
   * Perturbs the cluster with header specific but constant noise.
   *
   * @param noise List of noise for each Header
   */
  public void pertubeCluster(Map<String, Double> noise) {
    // Perturbs the cluster
    for (DocaItem item : this.contents) {
      for (Map.Entry<String, Double> data : item.getData().entrySet()) {
        // TODO: Instead of dataValue() use mean of cluster
        data.setValue(data.getValue() + noise.get(data.getKey()));
      }
    }
  }

  /**
   * Inserts a tuple into the cluster.
   *
   * @param element The element to insert into the cluster
   */
  public void insert(DocaItem element) {
    // checks for an empty cluster
    boolean firstElem = this.contents.isEmpty();
    this.contents.add(element);

    // Check whether the item is already in a cluster
    if (element.getCluster() != null) {
      // If it is, remove it so that we do not reach an invalid state
      element.getCluster().remove(element);
    }

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

  public void remove(DocaItem element) {
    this.contents.remove(element);
    element.setCluster(null);
  }

  /**
   * Calculates the enlargement value for adding item into this cluster.
   *
   * @param item         The tuple to calculate enlargement based on
   * @param globalRanges The globally known ranges for each attribute
   * @return The information loss if we added item into this cluster
   */
  public Double tupleEnlargement(DocaItem item, HashMap<String, Range<Double>> globalRanges) {
    Double given = this.informationLossGivenT(item, globalRanges);
    return given  / this.ranges.size();
  }

  /**
   * Calculates the information loss upon adding item into this cluster.
   *
   * @param item          The tuple to calculate information loss based on
   * @param globalRanges The globally known ranges for each attribute
   * @return The information loss given that we insert item into this cluster
   */
  private Double informationLossGivenT(DocaItem item,
                                       HashMap<String, Range<Double>> globalRanges) {
    Double loss = 0.0;
    if (this.contents.isEmpty()) {
      return 0.0;
    }
    // For each range, check if <item> would extend it
    Range<Double> updated;
    for (Map.Entry<String, Range<Double>> header : this.ranges.entrySet()) {
      Range<Double> ranges = globalRanges.get(header.getKey());
      updated =
          Range.between(
              (Math.min(header.getValue().getMinimum(), item.getData().get(header.getKey()))),
              Math.max(header.getValue().getMaximum(), item.getData().get(header.getKey())));
      loss += this.utils.doubleRangeInformationLoss(updated, ranges, this.headerWeights.get(header.getKey()));
    }
    return loss;
  }

  /**
   * Calculates the information loss upon merging cluster into this cluster.
   *
   * @param cluster       The cluster to calculate information loss based on
   * @param globalRanges The globally known ranges for each attribute
   * @return The information loss given that we merge cluster with this cluster
   */
  public Double informationLossGivenC(DocaCluster cluster, HashMap<String,
      Range<Double>> globalRanges) {
    Double loss = 0.0;
    if (this.contents.isEmpty()) {
      return cluster.informationLoss(globalRanges);
    } else if (cluster.contents.isEmpty()) {
      return this.informationLoss(globalRanges);
    }
    // For each range, check if <item> would extend it
    Range<Double> updated;
    for (Map.Entry<String, Range<Double>> header : this.ranges.entrySet()) {
      Range<Double> range = globalRanges.get(header.getKey());
      updated =
          Range.between(
              Math.min(
                  header.getValue().getMinimum(), cluster.ranges.get(header.getKey()).getMinimum()),
              Math.max(
                  header.getValue().getMaximum(),
                  cluster.ranges.get(header.getKey()).getMaximum()));
      loss += this.utils.doubleRangeInformationLoss(updated, range, this.headerWeights.get(header.getKey()));
    }
    return loss;
  }



}
