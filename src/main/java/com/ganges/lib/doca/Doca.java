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
  private final double LAMBDA = 0.003;      // tolerance parameter for domain building
  private final int beta;   // maximum number of clusters that can be stored
  private final int delayConstraint;
  // List of all current items (bound by delay constraint)
  // List of all upper bounds for each attribute/header
  private final double delta;   // publishing rate [0,1], 0 means no publishing
  //-----------Attributes for DOCA Phase----------------//
  private final List<Cluster> clusterList;  // List of all current active clusters
  private final HashMap<String, Range<Float>> rangeMap = new HashMap<>();
  // List of all ranges for each attribute/header
  private final List<CGItem> currentItems = new ArrayList<>();
  // could be potentially also just used as lower bound (if domains wont get stable)
  private static final int processingWindowSize = 10;
  //-----------Parameters for DOCA Phase----------------//
  private final double eps; // privacy budget
  private final boolean inplace;
  private final List<Double> losses = new ArrayList<>();      // Losses to use when calculating tau
  //-----------Attributes for DELTA Phase----------------//
  private List<List<Double>> stableDomain;
  // number of tuples that have to be collected before a phase begins with checking the release requirements
  private double tau; // average loss of last M expired clusters


  public Doca() {
    String[] parameters = getParameters();
    this.eps = Double.parseDouble(parameters[0]);
    this.delayConstraint = Integer.parseInt(parameters[1]);
    this.beta = Integer.parseInt(parameters[2]);
    this.inplace = Boolean.parseBoolean(parameters[3]);
    this.delta = Double.parseDouble(parameters[4]);
    this.stableDomain = new ArrayList<>();
    //this.GKQuantileEstimator = new GreenwaldKhannaQuantileEstimator(GKQError);
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
   * Added Tuple gets either suppressed or released to the DOCA Phase
   *
   * @param x Input Tuple
   */
  public double[][] addData(double[][] x) {
    List<List<Double>> domain = new ArrayList<>();
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

    double[][] output = new double[0][0];
    List<CGItem> anonymizedData = new ArrayList<>();
    for (List<List<Double>> stableDomain : stableDomains) {
      List<CGItem> itemList = DocaUtil.dataPointsToItems(stableDomain);

      // TODO: calculate sensitivity for each Header
      List<Double> maximums = DocaUtil.getMax(x);
      List<Double> minimums = DocaUtil.getMin(x);

      List<Double> sensitivityList = new ArrayList<>();
      for (int i = 0; i < maximums.size(); i++) {
        sensitivityList.add(Math.abs(minimums.get(i) - maximums.get(i)));
      }

      // If delta is != 0 calculate specific sensitivity
            /*
            if (delta != 0) {
                // calculate sensitivity
                List<Float> firstElem = new ArrayList<>(itemList.get(0).getData().values());
                List<Float> lastElem = new ArrayList<>(itemList.get(itemList.size() - 1).getData().values());

                float ac = Math.abs(firstElem.get(1) - lastElem.get(1));
                float cb = Math.abs(firstElem.get(0) - lastElem.get(0));


                sensitivity = (float) Math.hypot(ac, cb);

                System.out.println("Sensitivity: " + sensitivity);
                System.out.println("Domain Size: " + itemList.size());
            }
             */
      List<CGItem> pertubedItems = new ArrayList<>();
      for (CGItem item : itemList) {
        List<CGItem> returnItems = this.doca(item, sensitivityList, false);
        pertubedItems.addAll(returnItems);

        if (returnItems != null) {
          for (CGItem items : returnItems) {
            System.out.println(items.getData().values());
          }
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

    // Create Output structure
    List<CGItem> output = releasedItems;
    //TODO: what is this used for?
    //if (this.inplace != null) {
    //    output = this.inplace;
    //} else {
    //    output = new ArrayList<>();
    //}

    return output;
  }

  /**
   * Online Clustering Algorithm
   *
   * @param tuple Data Tuple to be clustered
   * @return List of expired clusters, the list is empty if no cluster expired
   */
  private Cluster onlineClustering(CGItem tuple) {
    // Find best cluster
    Cluster best_cluster = this.findBestCluster(tuple, DocaUtil.getAttributeDiff(this.rangeMap));

    if (best_cluster == null) {
      // Add new Cluster
      Cluster new_cluster = new Cluster(tuple.getHeaders());
      new_cluster.insert(tuple);
      this.clusterList.add(new_cluster);

    } else {
      // Add tuple to cluster
      best_cluster.insert(tuple);
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
      currentItems.add(tuple);
      return null;
    } else {
      CGItem expiredTuple = currentItems.remove(0);
      currentItems.add(tuple);
      expiredCluster = expiredTuple.getCluster();
    }
    return expiredCluster;
  }

  /**
   * Find the best cluster for a given data point
   *
   * @param dataPoint Data point to be clustered
   * @param dif       Difference of global ranges
   * @return Best cluster for the data point or null if no fitting cluster was found
   */
  private Cluster findBestCluster(CGItem dataPoint, HashMap<String, Float> dif) {
    int best_cluster = -1;
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
    double min_enlarge;
    if (enlargement.size() == 0) {
      min_enlarge = Double.POSITIVE_INFINITY;
    } else {
      min_enlarge = enlargement.stream().min(Double::compare).get();
    }

    // Find clusters with minimum enlargement
    // and Find acceptable clusters (with overall loss <= tau)
    List<Integer> ok_clusters = new ArrayList<>();
    List<Integer> min_clusters = new ArrayList<>();
    for (int c = 0; c < clusters.size(); c++) {
      double enl = enlargement.get(c);
      if (enl == min_enlarge) {
        min_clusters.add(c);

        HashMap<String, Float> dif_cluster = new HashMap<>();
        for (Map.Entry<String, Range<Float>> clusterRange : clusters.get(c).getRanges()
            .entrySet()) {
          dif_cluster.put(clusterRange.getKey(),
              clusterRange.getValue().getMaximum() - clusterRange.getValue().getMinimum());
        }
        double overall_loss = (enl +
            DocaUtil.divisionWith0(dif_cluster, dif).values().stream().reduce(0f, Float::sum) /
                numAttributes);
        if (overall_loss <= this.tau) {
          ok_clusters.add(c);
        }
      }
    }
    // First try to find a cluster with minimum enlargement and acceptable loss
    if (!ok_clusters.isEmpty()) {
      best_cluster = ok_clusters.stream()
          .min(Comparator.comparingInt(c -> clusters.get(c).getContents().size()))
          .orElse(-1);
      //If no new cluster is allowed, try to find a cluster with minimum enlargement
    } else if (clusters.size() >= beta) {
      best_cluster = min_clusters.stream()
          .min(Comparator.comparingInt(c -> clusters.get(c).getContents().size()))
          .orElse(-1);
    }

    if (best_cluster == -1) {
      return null;
    } else {
      return clusters.get(best_cluster);
    }
  }

  /**
   * Release an expired cluster after pertubation
   *
   * @param expiredCluster Cluster to be released
   * @param sensitivity    Sensitivity of the pertubation
   * @return True if the cluster was released, false if not
   */
  private List<CGItem> releaseExpiredCluster(Cluster expiredCluster, List<Double> sensitivity) {
    // update values
    HashMap<String, Float> dif = DocaUtil.getAttributeDiff(this.rangeMap);
    HashMap<String, Float> dif_cluster = new HashMap<>();
    for (Map.Entry<String, Range<Float>> clusterRange : expiredCluster.getRanges().entrySet()) {
      dif_cluster.put(clusterRange.getKey(),
          clusterRange.getValue().getMaximum() - clusterRange.getValue().getMinimum());
    }
    double loss =
        DocaUtil.divisionWith0(dif_cluster, dif).values().stream().reduce(0f, Float::sum) /
            (float) dif_cluster.keySet().size();
    this.losses.add(loss);
    //TODO: tau should only be calculated from the last m losses
    this.tau = this.losses.stream().mapToDouble(Double::doubleValue).sum() / losses.size();
    // release cluster from list
    this.clusterList.remove(expiredCluster);

    // pertube cluster items
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
    //sensitivity = 100f;
    //double scale = (sensitivity / (expiredCluster.getContents().size() * eps));
    //TODO: Create scale from difference of global ranges
    //double scale = expiredCluster.getContents().size() * eps;
    HashMap<String, Float> laplace = new HashMap<>();
    int headerNumber = 0;
    for (String attribute : mean.keySet()) {
      //laplace.put(attribute, (float) (Math.random() - 0.5));
      double scale = (sensitivity.get(headerNumber) / (expiredCluster.getContents().size() * eps));

      JDKRandomGenerator rg = new JDKRandomGenerator();
      LaplaceDistribution laplaceDistribution = new LaplaceDistribution(rg, 0, scale);
      float noise = (float) laplaceDistribution.sample();
      laplace.put(attribute, noise);
    }

    List<Float> noise = new ArrayList<>();

    for (String attribute : mean.keySet()) {
      noise.add(laplace.get(attribute));
    }

    //expiredCluster.getContents().stream().map(Item::getData).collect(Collectors.toList()).stream().filter(data. -> mean.get(item.));
    for (CGItem i : expiredCluster.getContents()) {
      for (Map.Entry<String, Float> entry : i.getData().entrySet()) {
        entry.setValue(mean.get(entry.getKey()));
      }
    }

    // Originale Values - Only for TESTING
    List<CGItem> originalItems = new ArrayList<>(expiredCluster.getContents());
    List<Float> originalValues = new ArrayList<>();
    for (CGItem i : originalItems) {
      for (Map.Entry<String, Float> e : i.getData().entrySet()) {
        originalValues.add(e.getValue());
      }
    }

    expiredCluster.pertubeCluster(noise);

    return expiredCluster.getContents();
  }

  @Override
  public List<AnonymizationItem> anonymize(List<AnonymizationItem> X) {
    if (X.size() == 0 || X.get(0).getValues().size() == 0) {
      return new ArrayList<>();
    }

    // Preserving fields order through anonymization input/output, since doca doesnt handle
    // field names in any way
    ArrayList<String> headers = new ArrayList<>(X.get(0).getValues().keySet());

    // Convert map to double array
    double[][] docaInput = new double[X.size()][];
    for (int i = 0; i < X.size(); i++) {
      docaInput[i] = new double[X.get(i).getValues().size()];
      for (int headerId = 0; headerId < headers.size(); headerId++) {
        docaInput[i][headerId] = X.get(i).getValues().get(headers.get(headerId));
      }
    }

    //double[][] result = addData(docaInput);
    double[][] result = anonymize(docaInput);

    // Convert double array to Anonymization Items
    // Doca outputs values in the same order as input, so we can use data from items with
    // same index in input list
    List<AnonymizationItem> outputResult = new ArrayList<>();
    for (int i = 0; i < result.length; i++) {
      Map<String, Double> dataRowMap = new LinkedHashMap<>();
      for (int headerId = 0; headerId < headers.size(); headerId++) {
        dataRowMap.put(headers.get(headerId), result[i][headerId]);
      }
      AnonymizationItem item = new AnonymizationItem(X.get(i).getId(), dataRowMap,
          X.get(i).getNonAnonymizedValues());
      outputResult.add(item);
    }
    if (outputResult.isEmpty()) {
      return outputResult;
    } else {
      // We return only one last item, since previous were output in previous calls
      return List.of(outputResult.get(outputResult.size() - 1));
    }
  }


  public double[][] anonymize(double[][] x) {
    String[] parameters = getParameters();
    double eps = Double.parseDouble(parameters[0]);
    int delay_constraint = Integer.parseInt(parameters[1]);
    int beta = Integer.parseInt(parameters[2]);
    boolean inplace = Boolean.parseBoolean(parameters[3]);
    double delta = Double.parseDouble(parameters[4]);
    int num_instances = x.length;
    int num_attributes = x[0].length;

    //double sensitivity = Math.abs((DocaUtil.getMax(x) - DocaUtil.getMin(x)));
    double sensitivity = 1.0;

    List<List<Integer>> clusters = new ArrayList<>();
    List<List<Integer>> clusters_final = new ArrayList<>();

    // Cluster attribute minimum/maximum
    List<double[]> mn_c = new ArrayList<>();
    List<double[]> mx_c = new ArrayList<>();

    // Global Attribute Minimum/Maximum
    double[] mn = new double[num_attributes];
    double[] mx = new double[num_attributes];
    for (int i = 0; i < num_attributes; i++) {
      mn[i] = Double.POSITIVE_INFINITY;
      mx[i] = Double.NEGATIVE_INFINITY;
    }

    // Losses saved for tau
    List<Double> losses = new ArrayList<>();

    double tau = 0;

    // Create Output structure
    double[][] output;
    if (inplace) {
      output = x;
    } else {
      output = new double[num_instances][num_attributes];
    }

    int TODOREMOVE_Perfect = 0;

    for (int clock = 0; clock < num_instances; clock++) {
      if (clock % 1000 == 0) {
        System.out.println("Clock " + clock + " " + TODOREMOVE_Perfect);
      }

      double[] data_point = x[clock];

      // Update min/max
      for (int i = 0; i < num_attributes; i++) {
        mn[i] = Math.min(mn[i], data_point[i]);
        mx[i] = Math.max(mx[i], data_point[i]);
      }

      double[] dif = new double[num_attributes];
      for (int i = 0; i < num_attributes; i++) {
        dif[i] = mx[i] - mn[i];
      }

      // Find best Cluster
      Integer best_cluster = null;
      if (!clusters.isEmpty()) {
        // Calculate enlargement (the value is not yet divided by the number of attributes!)
        double[] enlargement = new double[clusters.size()];
        for (int c = 0; c < clusters.size(); c++) {
          double sum = 0;
          for (int i = 0; i < num_attributes; i++) {
            sum +=
                Math.max(0, data_point[i] - mx_c.get(c)[i])
                    - Math.min(0, data_point[i] - mn_c.get(c)[i]);
          }
          enlargement[c] = sum;
        }

        double min_enlarge = Double.MAX_VALUE;

        List<Integer> ok_clusters = new ArrayList<>();
        List<Integer> min_clusters = new ArrayList<>();

        for (int c = 0; c < clusters.size(); c++) {
          double enl = enlargement[c];
          if (enl == min_enlarge) {
            min_clusters.add(c);
            double overall_loss =
                (enl + DocaUtil.getSumOfElementsInArray(DocaUtil.divisionWith0(mx_c.get(c), dif))) /
                    num_attributes;
            if (overall_loss <= tau) {
              ok_clusters.add(c);
            }
          }
        }

        if (!ok_clusters.isEmpty()) {
          TODOREMOVE_Perfect += 1;
          best_cluster =
              ok_clusters.stream()
                  .min(
                      (c1, c2) -> Integer.compare(clusters.get(c1).size(), clusters.get(c2).size()))
                  .orElse(null);
        } else if (clusters.size() >= beta) {
          best_cluster =
              min_clusters.stream()
                  .min(
                      (c1, c2) -> Integer.compare(clusters.get(c1).size(), clusters.get(c2).size()))
                  .orElse(null);
        }
      }

      if (best_cluster == null) {
        // Add new Cluster
        List<Integer> new_cluster = new ArrayList<>();
        new_cluster.add(clock);
        clusters.add(new_cluster);

        // Set Min/Max of new Cluster
        mn_c.add(data_point.clone());
        mx_c.add(data_point.clone());
      } else {
        clusters.get(best_cluster).add(clock);
        // Update min/max
        double[] mn_cluster = mn_c.get(best_cluster);
        double[] mx_cluster = mx_c.get(best_cluster);
        mn_cluster[best_cluster] = Math.min(mn_cluster[best_cluster], data_point[best_cluster]);
        mx_cluster[best_cluster] = Math.max(mx_cluster[best_cluster], data_point[best_cluster]);
      }

      List<Integer> overripe_clusters = new ArrayList<>();
      for (int c = 0; c < clusters.size(); c++) {
        if (clusters.get(c).contains(clock - delay_constraint)) {
          overripe_clusters.add(c);
        }
      }
      assert overripe_clusters.size() <= 1
          : "Every datapoint should only be able to be in one cluster!?";
      if (!overripe_clusters.isEmpty()) {
        int c = overripe_clusters.get(0);
        double[] dif_cluster = new double[num_attributes];
        for (int i = 0; i < num_attributes; i++) {
          dif_cluster[i] = mx_c.get(c)[i] - mn_c.get(c)[i];
        }
        double loss = DocaUtil.getSumOfElementsInArray(DocaUtil.divisionWith0(dif_cluster, dif)) /
            num_attributes;
        losses.add(loss);
        clusters_final.add(clusters.get(c));
        clusters.remove(c);
        mn_c.remove(c);
        mx_c.remove(c);
      }
    }

    clusters_final.addAll(clusters);

    for (List<Integer> cs : clusters_final) {
      double[] mean = new double[num_attributes];
      for (int i : cs) {
        for (int j = 0; j < num_attributes; j++) {
          mean[j] += x[i][j];
        }
      }
      for (int j = 0; j < num_attributes; j++) {
        mean[j] /= cs.size();
      }
      double scale = (sensitivity / (cs.size() * eps));
      double[] laplace = new double[num_attributes];
      for (int j = 0; j < num_attributes; j++) {
        laplace[j] = Math.random() - 0.5;
      }
      for (int j = 0; j < num_attributes; j++) {
        output[cs.get(0)][j] = mean[j] + scale * laplace[j];
      }
    }

    return output;
  }
}
