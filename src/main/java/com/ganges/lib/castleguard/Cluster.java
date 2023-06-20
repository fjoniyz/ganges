package com.ganges.lib.castleguard;

import com.ganges.lib.castleguard.utils.Utils;
import java.util.*;
import org.apache.commons.lang3.Range;

public class Cluster {
  private final Utils utils;
  private List<Item> contents;
  private Map<String, Range<Float>> ranges;
  private Set<Float> diversity;
  private Map<String, Float> sampleValues;

  public Cluster(List<String> headers) {
    // Initialises the cluster
    this.contents = new ArrayList<>();
    this.ranges = new LinkedHashMap<>();
    // Ranges method -> in Python zero arguments and initialized with zeros
    headers.forEach(header -> this.ranges.put(header, Range.between(0F, 0F)));
    this.diversity = new HashSet<>();
    this.sampleValues = new HashMap<>();

    this.utils = new Utils();
  }

  public List<Item> getContents() {
    return this.contents;
  }

  public void setContents(List<Item> value) {
    //this.contents = value;
    for (Item item : value) {
      this.insert(item);
    }
  }

  public int getSize() {
    return this.contents.size();
  }

  public int getDiversitySize() {
    return this.diversity.size();
  }

  public Map<String, Range<Float>> getRanges() {
    return this.ranges;
  }

  public void setRanges(HashMap<String, Range<Float>> value) {
    this.ranges = value;
  }

  public Map<String, Float> getSampleValues() {
    return this.sampleValues;
  }

  public void setSampleValues(Map<String, Float> value) {
    this.sampleValues = value;
  }

  public Set<Float> getDiversity() {
    return this.diversity;
  }

  public void setDiversity(Set<Float> value) {
    this.diversity = value;
  }

  public void insert(Item element) {
    // Inserts a tuple into the cluster
    // Args:
    //            element (Item): The element to insert into the cluster

    // checks for an empty cluster
    boolean firstElem = this.contents.isEmpty();
    this.contents.add(element);

    // Check whether the item is already in a cluster
    if (element.getCluster() != null) {
      // If it is, remove it so that we do not reach an invalid state
      element.getCluster().remove(element);
    }
    // Add sensitive attribute value to the diversity of cluster
    this.diversity.add(element.getSensitiveAttr());
    element.setCluster(this);

    // in case of an empty Cluster the Ranges are set to the items values
    if (firstElem) {
      for (Map.Entry<String, Range<Float>> header : this.ranges.entrySet()) {
        header.setValue(
            Range.between(
                element.getData().get(header.getKey()), element.getData().get(header.getKey())));
      }
      // Otherwise we search for the Minimum /Maximum
    } else {
      for (Map.Entry<String, Range<Float>> header : this.ranges.entrySet()) {
        header.setValue(
            Range.between(
                Math.min(header.getValue().getMinimum(), element.getData().get(header.getKey())),
                Math.max(header.getValue().getMaximum(), element.getData().get(header.getKey()))));
      }
    }
  }
// TODO: Range
  public void remove(Item element) {
    //  Removes a tuple from the cluster
    //  Args:
    //            element: The element to remove from the cluster


    this.contents.remove(element);

    element.setCluster(null);

    boolean containsSensitiveAttr = false;
    for (Item e : this.contents) {
      if (Objects.equals(e.getSensitiveAttr(), element.getSensitiveAttr())) {
        containsSensitiveAttr = true;
        break;
      }
    }
    if (!containsSensitiveAttr) {
      this.diversity.remove(element.getSensitiveAttr());
    }

    for (Map.Entry<String, Range<Float>> header : this.ranges.entrySet()) {
      header.setValue(
              Range.between(
                      this.findMinimum(header.getKey()), this.findMaximum(header.getKey())));
    }
  }

  // Note: Return value with only Item -> In Cluster.py return value (gen_tuple, item)
  Item generalise(Item item) {
    for (Map.Entry<String, Range<Float>> header : this.ranges.entrySet()) {
      if (!this.sampleValues.containsKey(header.getKey())) {
        this.sampleValues.put(
            header.getKey(), this.utils.randomChoice(this.contents).getData().get(header.getKey()));
      }
      item.removeHeaders("pid");
      item.getData().put("min" + header.getKey(), header.getValue().getMinimum());
      item.getData().put("spc" + header.getKey(), this.sampleValues.get(header.getKey()));
      item.getData().put("max" + header.getKey(), header.getValue().getMaximum());

      item.addHeaders("min" + header.getKey());
      item.addHeaders("spc" + header.getKey());
      item.addHeaders("max" + header.getKey());

      item.removeHeaders(header.getKey());
      item.removeData(header.getKey());
    }
    return item;
  }

  public float tupleEnlargement(Item item, HashMap<String, Range<Float>> globalRanges) {
    /*Calculates the enlargement value for adding <item> into this cluster

        Args:
            item: The tuple to calculate enlargement based on
            globalRanges: The globally known ranges for each attribute

    Returns: The information loss if we added item into this cluster
           */
    float given = this.informationLossGivenT(item, globalRanges);
    float current = this.informationLoss(globalRanges);
    return (given - current) / this.ranges.size();
  }

  public float clusterEnlargement(Cluster cluster, HashMap<String, Range<Float>> globalRanges) {
    /*Calculates the enlargement value for merging <cluster> into this cluster

    Args:
    cluster: The cluster to calculate information loss for
    globalRanges: The globally known ranges for each attribute

    Returns: The information loss upon merging cluster with this cluster*/
    Float given = this.informationLossGivenC(cluster, globalRanges);
    Float current = this.informationLoss(globalRanges);
    return (given - current) / this.ranges.size();
  }

  float informationLossGivenT(Item item, HashMap<String, Range<Float>> global_ranges) {
    /*Calculates the information loss upon adding <item> into this cluster

    Args:
    item: The tuple to calculate information loss based on
    global_ranges: The globally known ranges for each attribute

    Returns: The information loss given that we insert item into this cluster*/
    float loss = 0F;
    if (this.contents.isEmpty()){
      return 0.0F;
    }
    // For each range, check if <item> would extend it
    Range<Float> updated;
    for (Map.Entry<String, Range<Float>> header : this.ranges.entrySet()) {
      Range<Float> global_range = global_ranges.get(header.getKey());
      updated =
          Range.between(
              (Math.min(header.getValue().getMinimum(), item.getData().get(header.getKey()))),
              Math.max(header.getValue().getMaximum(), item.getData().get(header.getKey())));
      loss += this.utils.rangeInformationLoss(updated, global_range);
    }
    return loss;
  }

  public float informationLossGivenC(Cluster cluster, HashMap<String, Range<Float>> global_ranges) {
    /* Calculates the information loss upon merging <cluster> into this cluster

    Args:
        cluster: The cluster to calculate information loss based on
        global_ranges: The globally known ranges for each attribute

    Returns: The information loss given that we merge cluster with this cluster*/
    float loss = 0.0F;
    if (this.contents.isEmpty()){
      return cluster.informationLoss(global_ranges);
    }else if (cluster.contents.isEmpty()){
      return this.informationLoss(global_ranges);
    }
    // For each range, check if <item> would extend it
    Range<Float> updated;
    for (Map.Entry<String, Range<Float>> header : this.ranges.entrySet()) {
      Range<Float> global_range = global_ranges.get(header.getKey());
      updated =
          Range.between(
              Math.min(
                  header.getValue().getMinimum(), cluster.ranges.get(header.getKey()).getMinimum()),
              Math.max(
                  header.getValue().getMaximum(),
                  cluster.ranges.get(header.getKey()).getMaximum()));
      loss += this.utils.rangeInformationLoss(updated, global_range);
    }
    return loss;
  }

  public float informationLoss(HashMap<String, Range<Float>> global_ranges) {
    /*Calculates the information loss of this cluster

    Args:
    global_ranges: The globally known ranges for each attribute

    Returns: The current information loss of the cluster*/
    float loss = 0F;

    // For each range, check if <item> would extend it
    Range<Integer> updated = null;
    for (Map.Entry<String, Range<Float>> header : this.ranges.entrySet()) {
      Range<Float> global_range = global_ranges.get(header.getKey());
      loss += this.utils.rangeInformationLoss(header.getValue(), global_range);
    }
    return loss;
  }

  public float distance(Item other) {
    /*Calculates the distance from this tuple to another

    Args:
    other: The tuple to calculate the distance to

    Returns: The distance to the other tuple*/
    float total_distance = 0;
    for (Map.Entry<String, Range<Float>> header : this.ranges.entrySet()) {
      total_distance +=
          Math.abs(
              other.getData().get(header.getKey()) - this.utils.rangeDifference(header.getValue()));
    }
    return total_distance;
  }

  public boolean withinBounds(Item item) {
    /* Checks whether a tuple is within all the ranges of the
    cluster, e.g. would cause no information loss on being entered.

            Args:
    item: The tuple to perform bounds checking on

    Returns: Whether the tuple is within the bounds of the cluster*/
    for (Map.Entry<String, Range<Float>> header : this.ranges.entrySet()) {
      if (!header.getValue().contains(item.getData().get(header.getKey()))) {
        return false;
      }
    }
    return true;
  }

  public float findMinimum(String header){

    /* Finds a minimum value for the range within a cluster for a given header

            Args:
    header: Header as string

    Returns: the minimum value*/

    float minValue = Float.MAX_VALUE;
    for (Item item : this.getContents()){
      float value = item.getData().get(header);

      if (value < minValue) {
        minValue = value;
      }
    }
    return minValue;
  }

  public float findMaximum(String header){

    /* Finds a maximum value for the range within a cluster for a given header

            Args:
    header: Header as string

    Returns: the maximum value*/

    float maxValue = Float.MIN_VALUE;
    for (Item item : this.getContents()){
      float value = item.getData().get(header);

      if (value > maxValue) {
        maxValue = value;
      }
    }
    return maxValue;
  }
}
