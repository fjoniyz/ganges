package com.ganges.anonlib.castleguard.utils;

import com.ganges.anonlib.castleguard.CGItem;
import com.ganges.anonlib.castleguard.CGCluster;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.commons.lang3.Range;

public class ClusterManagement {

  private final int k;
  private final int l;
  private final int mu;
  private final List<CGCluster> bigGamma = new ArrayList<>(); // Set of non-ks anonymised clusters
  private final List<CGCluster> bigOmega = new ArrayList<>(); // Set of ks anonymised clusters
  private final List<String> headers;
  private final String sensitiveAttribute;
  private final List<Double> recentLosses = new ArrayList<>();
  private double tau;

  public ClusterManagement(int k, int l, int mu, List<String> headers, String sensitiveAttribute) {
    this.k = k;
    this.l = l;
    this.mu = mu;
    this.headers = headers;
    this.sensitiveAttribute = sensitiveAttribute;
  }

  public List<CGCluster> getNonAnonymizedClusters() {
    return bigGamma;
  }

  public List<CGCluster> getAnonymizedClusters() {
    return bigOmega;
  }

  public void addToNonAnonymizedClusters(CGCluster c) {
    this.bigGamma.add(c);
  }

  public void addToAnonymizedClusters(CGCluster c) {
    this.bigOmega.add(c);
  }

  public void removeFromNonAnonymizedClusters(CGCluster c) {
    this.bigGamma.remove(c);
  }

  public void removeFromAnonymizedClusters(CGCluster c) {
    this.bigOmega.remove(c);
  }

  /**
   * Split cluster into smaller clusters if possible and return the list of clusters
   * if not possible list will contain only the original cluster.
   *
   * @param c            Cluster to split
   * @param headers      Headers of the dataset
   * @param globalRanges Global ranges of the dataset
   * @return List of clusters
   */
  public List<CGCluster> splitL(CGCluster c, List<String> headers,
                                HashMap<String, Range<Double>> globalRanges) {
    List<CGCluster> sc = new ArrayList<>();

    // Group every tuple by the sensitive attribute and pid
    Map<Double, List<CGItem>> buckets = generateBuckets(c);

    // If number of buckets (sensitive Attributes) is smaller than l, return the cluster
    if (buckets.size() < l) {
      sc.add(c);
      return sc;
    }

    // Check the number of distinct PIDs in the buckets
    Set<Double> availablePids = new HashSet<>();
    for (List<CGItem> bucket : buckets.values()) {
      Set<Double> ids = bucket.stream().map(CGItem::getPid).collect(Collectors.toSet());
      availablePids.addAll(ids);
    }

    int count = availablePids.size();

    // While length of buckets greater than l and more than k tuples with distinct PIDs
    while (buckets.size() >= l && count >= k) {

      // Pick a random tuple from a random bucket (bucketKeys = sensitive Attributes)
      List<Double> bucketKeys = new ArrayList<>(buckets.keySet());
      Random random = new Random();
      Double randomSensitiveAttribute = bucketKeys.get(random.nextInt(bucketKeys.size()));

      List<CGItem> bucket = buckets.get(randomSensitiveAttribute);
      CGItem t = bucket.remove(random.nextInt(bucket.size()));

      // Create a new subcluster over t
      CGCluster cnew = new CGCluster(headers, c.getHeaderWeights());
      cnew.insert(t);
      availablePids.remove(t.getPid());

      if (bucket.isEmpty()) {
        buckets.remove(randomSensitiveAttribute);
      }

      List<Double> emptyBuckets = new ArrayList<>();

      // Go through each bucket. Sort the bucket by the enlargement value of that cluster. Insert the calculated amount of tuples in a new cluster
      for (Map.Entry<Double, List<CGItem>> entry : buckets.entrySet()) {
        Double currentSensitiveAttribute = entry.getKey();
        List<CGItem> currentBucket = entry.getValue();

        // Sort the bucket by the enlargement value of that cluster - use only the tuples with available PIDs (unused in this bucket/distinct)
        List<CGItem> availableTuples = currentBucket.stream()
            .filter(item -> availablePids.contains(item.getPid()))
            .sorted(Comparator.comparingDouble(tuple -> cnew.tupleEnlargement(tuple, globalRanges)))
            .collect(Collectors.toList());

        // Count the number of tuples we have
        int totalTuples = 0;
        for (List<CGItem> b : buckets.values()) {
          totalTuples += b.size();

          // EXPERIMENTAL - count only the tuples with available PIDs
          // totalTuples += b.stream().filter(item -> availablePids.contains(item.getPid())).collect(Collectors.toSet()).size();
        }

        // Calculate the number of tuples we should take
        int chosenCount = (int) Math.max(k * (currentBucket.size() / (double) totalTuples), 1);

        // EXPERIMENTAL - count only the tuples with available PIDs
        // int chosenCount = (int) Math.max(k * (availableTuples.size() / (double) totalTuples), 1);

        // Get subset Tj of "choosenCount" tuples from the bucket
        List<CGItem> subset =
            availableTuples.subList(0, Math.min(chosenCount, availableTuples.size()));

        // Insert the top Tj tuples in a new cluster
        for (CGItem item : subset) {
          availablePids.remove(item.getPid());
          cnew.insert(item);
          currentBucket.remove(item);
        }

        // if bucket is empty delete the bucket
        if (currentBucket.isEmpty()) {
          emptyBuckets.add(currentSensitiveAttribute);
        }
      }

      // remove empty buckets
      for (Double emptyBucketKey : emptyBuckets) {
        buckets.remove(emptyBucketKey);
      }

      // Reset the available PIDs and the count
      availablePids.clear();
      for (List<CGItem> b : buckets.values()) {
        List<Double> ids = b.stream().map(CGItem::getPid).toList();
        availablePids.addAll(ids);
      }
      count = availablePids.size();

      // Add the new cluster to the list of clusters or insert the tuples back into the buckets
      if (cnew.getKSize() >= k && cnew.getDiversity().size() >= l) {
        sc.add(cnew);
      } else {
        for (CGItem item : cnew.getContents()) {
          if (!buckets.containsKey(item.getSensitiveAttr())) {
            buckets.put(item.getSensitiveAttr(), new ArrayList<>());
          }
          buckets.get(item.getSensitiveAttr()).add(item);
        }
      }
    }

    if (sc.isEmpty()) {
      sc.add(c);
      return sc;
    }

    // Add all remaining tuples from the buckets to the nearest newly created cluster
    for (List<CGItem> bucket : buckets.values()) {
      for (CGItem t : bucket) {
        CGCluster CGCluster =
            Collections.min(sc, Comparator.comparingDouble(cluster1 -> cluster1.distance(t)));
        CGCluster.insert(t);
      }
      bucket.clear();
    }
    buckets.clear();

    // Add remaining tuples from old cluster to clusters that contain tuple(s) with identical pid
    for (CGCluster splittedCGCluster : sc) {
      List<CGItem> tuplesWithSamePID = new ArrayList<>();
      for (CGItem tuple : splittedCGCluster.getContents()) {
        List<CGItem> itemsToRemove = new ArrayList<>();
        for (CGItem tupleFromOriginalCluster : c.getContents()) {
          if (tupleFromOriginalCluster.getPid().equals(tuple.getPid())) {
            tuplesWithSamePID.add(tupleFromOriginalCluster);
            itemsToRemove.add(tupleFromOriginalCluster);
          }
        }
        c.getContents().removeAll(itemsToRemove);
      }
      for (CGItem t : tuplesWithSamePID) {
        splittedCGCluster.insert(t);
      }
      // Add the new cluster to the bigGamma
      this.bigGamma.add(splittedCGCluster);
    }
    return sc;
  }


  /**
   * Groups all tuples in the cluster by their sensitive attribute selecting only one tuple
   * for each pid (no overlaps).
   * Implemented as described in the Paper
   *
   * @param CGCluster: The cluster to generate the buckets for
   * @return: A dictionary of attribute values to lists of items with those values
   */
  private Map<Double, List<CGItem>> generateBuckets(CGCluster CGCluster) {
    Map<Double, List<CGItem>> buckets;
    buckets = new HashMap<>();

    Set<Double> pids = new HashSet<>();
    int numberOfPids = CGCluster.getContents().stream()
        .map(CGItem::getPid)
        .collect(Collectors.toSet())
        .size();

    // puts each tupel, that refers to a distinct person in a bucket according to its sensitive Attribute
    // !! Each PID is only picked once !!
    while (pids.size() < numberOfPids) {
      Random random = new Random();
      CGItem t = CGCluster.getContents().get(random.nextInt(CGCluster.getSize()));

      // Get the value for the sensitive attribute for this tuple
      Double sensitiveValue = t.getData().get(this.sensitiveAttribute);
      Double currentPID = t.getPid();

      if (!pids.contains(currentPID)) {

        pids.add(currentPID);

        // If it isn't in our map, make an empty list for it
        if (!buckets.containsKey(sensitiveValue)) {
          buckets.put(sensitiveValue, new ArrayList<>());
        }

        // Insert the tuple into the cluster
        buckets.get(sensitiveValue).add(t);
      }
    }
    return buckets;
  }


  /**
   * Merges a cluster with other clusters in big_gamma until the size of the resulting cluster is
   * larger than k
   *
   * @param c:           The cluster that needs to be merged
   * @param globalRanges
   * @return: A cluster with a size larger than or equal to k
   */
  public CGCluster mergeClusters(CGCluster c, HashMap<String, Range<Double>> globalRanges) {
    List<CGCluster> gamma_c = new ArrayList<>(this.bigGamma);
    gamma_c.remove(c);
    if (gamma_c.size() == 0) {
      return c;
    }

    while (c.getContents().size() < this.k || c.getDiversity().size() < this.l) {
      // Get the cluster with the lowest enlargement value
      CGCluster lowestEnlargementCGCluster =
          Collections.min(
              gamma_c, Comparator.comparingDouble(cl -> cl.clusterEnlargement(cl, globalRanges)));
      List<CGItem> items = new ArrayList<>(lowestEnlargementCGCluster.getContents());

      for (CGItem t : items) {
        c.insert(t);
      }

      this.bigGamma.remove(lowestEnlargementCGCluster);
      gamma_c.remove(lowestEnlargementCGCluster);
    }

    return c;
  }

  /**
   * Updates the local value of tau, depending on what state the algorithm is currently in.
   *
   * @param globalRanges
   */
  public void updateTau(HashMap<String, Range<Double>> globalRanges) {
    this.tau = Double.POSITIVE_INFINITY;
    if (!recentLosses.isEmpty()) {
      tau = recentLosses.stream().reduce(0.0, Double::sum);
    } else if (!bigGamma.isEmpty()) {
      int sampleSize = Math.min(bigGamma.size(), 5);
      List<CGCluster> chosen = Utils.randomChoice(bigGamma, sampleSize);

      double totalLoss =
          chosen.stream().map(c -> c.informationLoss(globalRanges)).reduce(0.0, Double::sum);
      tau = totalLoss / sampleSize;
    }
  }

  /**
   * Updates the infomation loss.
   *
   * @param c
   * @param globalRanges
   */
  public void updateLoss(CGCluster c, HashMap<String, Range<Double>> globalRanges) {
    double loss = c.informationLoss(globalRanges);
    recentLosses.add(loss);
    if (recentLosses.size() > this.mu) {
      recentLosses.remove(0);
    }
    this.updateTau(globalRanges);
  }
}
