package com.ganges.lib.doca;

import com.ganges.lib.AnonymizationAlgorithm;
import com.ganges.lib.AnonymizationItem;
import com.ganges.lib.DataRepository;
import com.ganges.lib.castleguard.CGItem;
import com.ganges.lib.castleguard.Cluster;
import com.ganges.lib.castleguard.utils.Utils;
import com.ganges.lib.doca.utils.DocaUtil;
import com.ganges.lib.doca.utils.GreenwaldKhannaQuantileEstimator;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import org.apache.commons.lang3.Range;
import org.apache.commons.math3.distribution.LaplaceDistribution;
import org.apache.commons.math3.random.JDKRandomGenerator;

public class Doca implements AnonymizationAlgorithm {

  //-----------Parameters for DELTA Phase----------------//
  private static final DataRepository domainRepository = new DataRepository();
  // List of all items that are part of the stable domain
  private static GreenwaldKhannaQuantileEstimator GKQuantileEstimator =
      new GreenwaldKhannaQuantileEstimator(0.02);   // GK Quantile Estimator
  private static List<Double> Vinf = new ArrayList<>();
  // List of all lower bounds for each attribute/header TODO: Check if ranges have to be correct implemented or change with ranges
  private static List<Double> Vsup = new ArrayList<>();
  // Size that has to be reached before release conditions for stable Domain are checked
  private final double LAMBDA = 0.8;      // tolerance parameter for domain building
  private final int beta;   // maximum number of clusters that can be stored
  private final int delayConstraint;
  // List of all current items (bound by delay constraint)
  // List of all upper bounds for each attribute/header
  private final double delta;   // publishing rate [0,1], 0 means everything is published
  //-----------Attributes for DOCA Phase----------------//
  private final List<Cluster> clusterList;  // List of all current active clusters
  private final HashMap<String, Range<Float>> rangeMap = new HashMap<>();
  // List of all ranges for each attribute/header
  private final List<CGItem> currentItems = new ArrayList<>();
  // could be potentially also just used as lower bound (if domains won't get stable)
  private static final int processingWindowSize = 25;
  //-----------Parameters for DOCA Phase----------------//
  private final double eps; // privacy budget
  private final boolean inplace;
  private final List<Double> losses = new ArrayList<>();      // Losses to use when calculating tau
  //-----------Attributes for DELTA Phase----------------//
  private List<List<Double>> stableDomain;
  // number of tuples that have to be collected before a phase begins with checking the release requirements
  private double tau; // average loss of last M expired clusters


  //test
  private static List<AnonymizationItem> leftOverItems = new ArrayList<>();


  public Doca() {
    String[] parameters = getParameters();
    this.eps = Double.parseDouble(parameters[0]);
    this.delayConstraint = Integer.parseInt(parameters[1]);
    this.beta = Integer.parseInt(parameters[2]);
    this.inplace = Boolean.parseBoolean(parameters[3]);
    this.delta = Double.parseDouble(parameters[4]);
    this.stableDomain = new ArrayList<>();
    this.tau = 0;
    this.clusterList = new ArrayList<>();
  }

  public static String[] getParameters() {
    String[] result = new String[5];
    String userDirectory = System.getProperty("user.dir");
    try (InputStream inputStream = Files.newInputStream(
        Paths.get(userDirectory + "/src/main/resources/doca.properties"))) {
      Properties properties = new Properties();
      properties.load(inputStream);
      result[0] = properties.getProperty("eps");
      result[1] = properties.getProperty("delay_constraint");
      result[2] = properties.getProperty("beta");
      result[3] = properties.getProperty("inplace");
      result[4] = properties.getProperty("delta");
      return result;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Added Tuple gets either suppressed or released to the DOCA Phase.
   *
   * @param x Input Tuple
   */
  public double[][] addData(double[][] x) {
    List<List<Double>> domain;
    List<List<Double>> dataPoints = new ArrayList<>();
    for (double[] l : x) {
      List<Double> headerData = new ArrayList<>();
      for (double v : l) {
        headerData.add(v);
      }
      dataPoints.add(headerData);
    }
    List<List<List<Double>>> stableDomains = new ArrayList<>();
    if (this.delta != 0) {
      for (List<Double> dataPoint : dataPoints) {
        domain = this.addToDomain(dataPoint);
        if (domain != null) {
          stableDomains.add(domain);
        }
      }
    } else {
      stableDomains.add(dataPoints);
    }

    double[][] output;
    List<CGItem> anonymizedData = new ArrayList<>();
    for (List<List<Double>> stableDomain : stableDomains) {
      List<CGItem> itemList = DocaUtil.dataPointsToItems(stableDomain);

      List<Double> maximums = DocaUtil.getMax(x);
      List<Double> minimums = DocaUtil.getMin(x);

      List<Double> sensitivityList = new ArrayList<>();
      for (int i = 0; i < maximums.size(); i++) {
        sensitivityList.add(Math.abs(minimums.get(i) - maximums.get(i)));
      }

      List<CGItem> pertubedItems = new ArrayList<>();
      for (CGItem item : itemList) {
        List<CGItem> returnItems = this.doca(item, sensitivityList, false);
        if (!returnItems.isEmpty()) {
          pertubedItems.addAll(returnItems);
        }
      }
      anonymizedData.addAll(pertubedItems);
    }
    // create output tuple
    int rows = anonymizedData.size();
    int cols = anonymizedData.get(0).getData().size();
    output = new double[rows][cols];

    for (int i = 0; i < rows; i++) {
      Map<String, Float> attributes = anonymizedData.get(i).getData();
      int j = 0;
      for (float value : attributes.values()) {
        output[i][j] = value;
        j++;
      }
    }
    return output;
  }

  /**
   * Adds a Tuple to the domain and checks if it is stable.
   * if domain is stable, it will be released
   *
   * @param x Tuple to be added
   * @return the domain if it is stable, otherwise null
   */
  protected List<List<Double>> addToDomain(List<Double> x) {
    double tolerance = this.LAMBDA;

    // EXPERIMENTAL: Use mean of tuple as value for GK
    // Get mean value of data point
    double sum = x.stream().reduce(0.0, Double::sum);
    double mean = sum / x.size();

    // Add tuple to GKQEstimator
    GKQuantileEstimator.add(mean, x);

    // Add Quantile to Vinf and Vsup
    // Key is index, value is value
    HashMap<Integer, Double> estQuantilInf =
        GKQuantileEstimator.getQuantile((1.0 - this.delta) / 2.0);
    HashMap<Integer, Double> estQuantilSup =
        GKQuantileEstimator.getQuantile((1.0 + this.delta) / 2.0);

    Vinf.add((double) estQuantilInf.values().toArray()[0]);
    Vsup.add((double) estQuantilSup.values().toArray()[0]);

    // Check if processing window is large enough
    if (Vinf.size() == processingWindowSize && Vsup.size() == processingWindowSize) {
      double stdVinf = DocaUtil.calculateStandardDeviation(Vinf);
      double stdVsup = DocaUtil.calculateStandardDeviation(Vsup);

      double meanVinf = Vinf.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
      double meanVsup = Vsup.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);

      //coefficient of variation
      double cvVinf = stdVinf / meanVinf;
      double cvVsup = stdVsup / meanVsup;

      // check if domain is stable
      if (cvVinf < tolerance && cvVsup < tolerance) {

        List<List<Double>> domain = GKQuantileEstimator.getDomain();
        int from = (int) estQuantilInf.keySet().toArray()[0];
        int to = (int) estQuantilSup.keySet().toArray()[0];
        this.stableDomain = domain.subList(from, to);


        Vinf = new ArrayList<>();
        Vsup = new ArrayList<>();
        GKQuantileEstimator = new GreenwaldKhannaQuantileEstimator(0.02);
        return this.stableDomain;
      }
      // remove oldest tuple to remain size of stable domain
      Vinf.remove(0);
      Vsup.remove(0);
    }
    return null;
  }

  /**
   * Adds a Tuple to the domain and checks if it is stable.
   * if domain is stable, it will be released
   *
   * @param dataTuple     Tuple to be added
   * @param sensitivities sensitivity of the tuple
   * @return the domain if it is stable, otherwise null
   */
  protected List<CGItem> doca(CGItem dataTuple, List<Double> sensitivities, boolean inplace) {
    // Add tuple to best cluster and return expired clusters if any
    Cluster expiringCluster = this.onlineClustering(dataTuple);

    List<CGItem> releasedItems = new ArrayList<>();
    // Release Cluster if expired
    if (expiringCluster != null) {
      releasedItems.addAll(releaseExpiredCluster(expiringCluster, sensitivities));
    }

    return releasedItems;
  }

  /**
   * Online Clustering Algorithm.
   *
   * @param tuple Data Tuple to be clustered
   * @return List of expired clusters, the list is empty if no cluster expired
   */
  private Cluster onlineClustering(CGItem tuple) {
    // Find best cluster
    Cluster bestCluster = this.findBestCluster(tuple, DocaUtil.getAttributeDiff(this.rangeMap));

    if (bestCluster == null) {
      // Add new Cluster
      Cluster newCluster = new Cluster(tuple.getHeaders());
      newCluster.insert(tuple);
      this.clusterList.add(newCluster);

    } else {
      // Add tuple to cluster
      bestCluster.insert(tuple);
    }

    // update global ranges
    for (String header : tuple.getHeaders()) {
      if (this.rangeMap.containsKey(header)) {
        Utils.updateRange(this.rangeMap.get(header), tuple.getData().get(header));
      } else {
        this.rangeMap.put(header, Range.is(tuple.getData().get(header)));
      }
    }

    Cluster expiredCluster;
    // add tuple to currently active items
    if (this.currentItems.size() <= this.delayConstraint) {
      this.currentItems.add(tuple);
      return null;
    } else {
      CGItem expiredTuple = this.currentItems.remove(0);
      currentItems.add(tuple);
      expiredCluster = expiredTuple.getCluster();
    }
    return expiredCluster;
  }

  /**
   * Find the best cluster for a given data point.
   *
   * @param dataPoint Data point to be clustered
   * @param dif       Difference of global ranges
   * @return Best cluster for the data point or null if no fitting cluster was found
   */
  private Cluster findBestCluster(CGItem dataPoint, HashMap<String, Float> dif) {
    int bestCluster = -1;
    int numAttributes = dataPoint.getHeaders().size();
    List<Cluster> clusters = this.clusterList;


    // Calculate enlargement (the value is not yet divided by the number of attributes!)
    List<Double> enlargement = new ArrayList<>();
    for (Cluster cluster : clusters) {
      double sum = 0;
      for (Map.Entry<String, Float> entry : dataPoint.getData().entrySet()) {
        String key = entry.getKey();
        Float value = entry.getValue();
        sum += Math.max(0, value - cluster.getRanges().get(key).getMaximum())
            - Math.min(0, value - cluster.getRanges().get(key).getMinimum());
      }
      enlargement.add(sum);
    }

    // Find minimum enlargement
    double minEnlarge;
    if (enlargement.isEmpty()) {
      minEnlarge = Double.POSITIVE_INFINITY;
    } else {
      minEnlarge = enlargement.stream().min(Double::compare).get();
    }

    // Find clusters with minimum enlargement
    // and Find acceptable clusters (with overall loss <= tau)
    List<Integer> okClusters = new ArrayList<>();
    List<Integer> minClusters = new ArrayList<>();
    for (int c = 0; c < clusters.size(); c++) {
      double enl = enlargement.get(c);
      if (enl == minEnlarge) {
        minClusters.add(c);

        HashMap<String, Float> difCluster = new HashMap<>();
        for (Map.Entry<String, Range<Float>> clusterRange : clusters.get(c).getRanges()
            .entrySet()) {
          difCluster.put(clusterRange.getKey(),
              clusterRange.getValue().getMaximum() - clusterRange.getValue().getMinimum());
        }
        double overallLoss = (enl 
            + DocaUtil.divisionWith0(difCluster, dif).values().stream().reduce(0f, Float::sum) 
            / numAttributes);
        if (overallLoss <= this.tau) {
          okClusters.add(c);
        }
      }
    }
    // First try to find a cluster with minimum enlargement and acceptable loss
    if (!okClusters.isEmpty()) {
      bestCluster = okClusters.stream()
          .min(Comparator.comparingInt(c -> clusters.get(c).getContents().size()))
          .orElse(-1);
      //If no new cluster is allowed, try to find a cluster with minimum enlargement
    } else if (clusters.size() >= beta) {
      bestCluster = minClusters.stream()
          .min(Comparator.comparingInt(c -> clusters.get(c).getContents().size()))
          .orElse(-1);
    }

    if (bestCluster == -1) {
      return null;
    } else {
      return clusters.get(bestCluster);
    }
  }

  /**
   * Release an expired cluster after perturbation.
   *
   * @param expiredCluster  Cluster to be released
   * @param sensitivityList Sensitivity for the perturbation of each header
   * @return an expired cluster or empty list if no cluster was released (e.g. delay constraint not
   *          reached).
   */
  private List<CGItem> releaseExpiredCluster(Cluster expiredCluster, List<Double> sensitivityList) {
    // update values
    HashMap<String, Float> dif = DocaUtil.getAttributeDiff(this.rangeMap);
    HashMap<String, Float> difCluster = new HashMap<>();
    for (Map.Entry<String, Range<Float>> clusterRange : expiredCluster.getRanges().entrySet()) {
      difCluster.put(clusterRange.getKey(),
          clusterRange.getValue().getMaximum() - clusterRange.getValue().getMinimum());
    }
    double loss =
        DocaUtil.divisionWith0(difCluster, dif).values().stream().reduce(0f, Float::sum) 
            / (float) difCluster.keySet().size();
    this.losses.add(loss);
    //TODO: tau should only be calculated from the last m losses
    this.tau = this.losses.stream().mapToDouble(Double::doubleValue).sum() / losses.size();
    // release cluster from list
    this.clusterList.remove(expiredCluster);

    // perturbs cluster items
    HashMap<String, Float> attr = new HashMap<>();
    for (CGItem item : expiredCluster.getContents()) {
      // Sum up each Attribute
      for (String header : item.getHeaders()) {
        if (!attr.containsKey(header)) {
          attr.put(header, 0f);
        }
        attr.put(header, attr.get(header) + item.getData().get(header));
      }
    }
    // Calculate mean of Attributes
    HashMap<String, Float> mean = new HashMap<>();
    for (Map.Entry<String, Float> attrEntry : attr.entrySet()) {
      mean.put(attrEntry.getKey(), attrEntry.getValue() / expiredCluster.getContents().size());
    }

    List<Float> noise = getNoise(expiredCluster, sensitivityList, mean);

    for (CGItem i : expiredCluster.getContents()) {
      for (Map.Entry<String, Float> entry : i.getData().entrySet()) {
        entry.setValue(mean.get(entry.getKey()));
      }
    }

    for (CGItem expired : expiredCluster.getContents()) {
      this.currentItems.removeIf(currentItem -> expired.getExternalId().equals(currentItem.getExternalId()));
    }
    expiredCluster.pertubeCluster(noise);
    return expiredCluster.getContents();
  }

  private List<Float> getNoise(Cluster expiredCluster, List<Double> sensitivityList,
                                HashMap<String, Float> mean) {
    HashMap<String, Float> laplace = new HashMap<>();
    int headerNumber = 0;
    for (String attribute : mean.keySet()) {
      double scale = (sensitivityList.get(headerNumber)
          / (expiredCluster.getContents().size() * eps));

      JDKRandomGenerator rg = new JDKRandomGenerator();
      LaplaceDistribution laplaceDistribution = new LaplaceDistribution(rg, 0, scale);
      float noise = (float) laplaceDistribution.sample();
      laplace.put(attribute, noise);
    }

    List<Float> noise = new ArrayList<>();

    for (String attribute : mean.keySet()) {
      noise.add(laplace.get(attribute));
    }
    return noise;
  }

  @Override
  public List<AnonymizationItem> anonymize(List<AnonymizationItem> x) {
    if (x.isEmpty() || x.get(0).getValues().isEmpty()) {
      return new ArrayList<>();
    }

    // Preserving fields order through anonymization input/output, since doca doesn't handle
    // field names in any way
    ArrayList<String> headers = new ArrayList<>(x.get(0).getValues().keySet());

    // Convert map to double array
    double[][] docaInput = new double[x.size()][];
    for (int i = 0; i < x.size(); i++) {
      docaInput[i] = new double[x.get(i).getValues().size()];
      for (int headerId = 0; headerId < headers.size(); headerId++) {
        docaInput[i][headerId] = x.get(i).getValues().get(headers.get(headerId));
      }
    }

    double[][] result = addData(docaInput);

    // Add remaining items to leftOverItems list
    for (CGItem remainingItem : this.currentItems) {
      for (AnonymizationItem origItem : x) {
        if (Objects.equals(origItem.getId(), remainingItem.getExternalId())) {
          leftOverItems.add(origItem);
        }
      }
    }

    // TODO: Instead of clearing the list save leftover Tuples for next run
    this.currentItems.clear();
    this.clusterList.clear();

    // Convert double array to Anonymization Items
    // Doca outputs values in the same order as input, so we can use data from items with
    // same index in input list
    List<AnonymizationItem> outputResult = new ArrayList<>();
    for (int i = 0; i < result.length; i++) {
      Map<String, Double> dataRowMap = new LinkedHashMap<>();
      for (int headerId = 0; headerId < headers.size(); headerId++) {
        dataRowMap.put(headers.get(headerId), result[i][headerId]);
      }
      AnonymizationItem item = new AnonymizationItem(x.get(i).getId(), dataRowMap,
          x.get(i).getNonAnonymizedValues());
      outputResult.add(item);
    }

    return outputResult;
  }
}
