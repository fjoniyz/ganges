package com.ganges.lib.doca;

import com.ganges.lib.AnonymizationAlgorithm;
import com.ganges.lib.AnonymizationItem;
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
import java.util.stream.Collectors;
import org.apache.commons.lang3.Range;
import org.apache.commons.math3.distribution.LaplaceDistribution;
import org.apache.commons.math3.random.JDKRandomGenerator;

public class Doca implements AnonymizationAlgorithm {

  //-----------Parameters/Variables for DELTA Phase----------------//
  private static GreenwaldKhannaQuantileEstimator GKQuantileEstimator =
      new GreenwaldKhannaQuantileEstimator(0.002);    // GK Quantile Estimator
  private static List<Double> Vinf = new ArrayList<>(); // List of all lower bounds for each header
  private static List<Double> Vsup = new ArrayList<>();  // List of all upper bounds for each header
  private final double LAMBDA = 0.8;      // tolerance parameter for domain building
  private final int beta;   // maximum number of clusters that can be stored
  private final int delayConstraint;  // maximum time a tuple can be stored before it's released
  private final double delta;   // suppression rate
  private boolean stableDomainReached = false;
  private static final int processingWindowSize = 400; //size of process window for domain bounding
  private Map<DocaItem, Double> sortedCurrentDomain = new HashMap<>(); // Sorted Map of current domain

  //-----------Parameters/Variables for DOCA Phase----------------//
  private List<DocaCluster> clusterList;  // List of all current active clusters
  private HashMap<String, Range<Double>> rangeMap = new HashMap<>(); // Global ranges
  private final List<DocaItem> currentItems = new ArrayList<>();
  private final double eps; // privacy budget
  private final List<Double> losses = new ArrayList<>();   // Losses to use when calculating tau
  private double tau; // average loss of last M expired clusters
  private List<DocaItem> domain;
  private final Map<String, Double> sensitivityList = new HashMap<>();
  private Map<String, Double> headerWeights;


  public Doca() {
    String[] parameters = getParameters();
    this.eps = Double.parseDouble(parameters[0]);
    this.delayConstraint = Integer.parseInt(parameters[1]);
    this.beta = Integer.parseInt(parameters[2]);
    this.delta = Double.parseDouble(parameters[4]);
    this.domain = new ArrayList<>();
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

  @Override
  public List<AnonymizationItem> anonymize(List<AnonymizationItem> x) {
    // Initialize header weights
    this.headerWeights = x.get(0).getHeaderWeights().entrySet()
        .stream()
        .collect(Collectors.toMap(
            Map.Entry::getKey,
            entry -> entry.getValue().doubleValue() // Convert Float to Double
        ));

    if (x.isEmpty() || x.get(0).getValues().isEmpty()) {
      return new ArrayList<>();
    }

    // Alternative Doca-Implementation (is triggered when history data is send with the message)
    if (x.size() > 1) {
      // Preserving fields order through anonymization input/output, since doca doesnt handle
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
      double[][] result = docaAlternative(docaInput);
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
      if (outputResult.isEmpty()) {
        return outputResult;
      } else {
        // We return only one last item, since previous were output in previous calls
        return List.of(outputResult.get(outputResult.size()-1));
      }
    }

    // Doca Implementation
    for (AnonymizationItem docaInput : x) {
      DocaItem currentItem = new DocaItem(docaInput.getId(), docaInput.getValues(), docaInput.getNonAnonymizedValues(),
          docaInput.getValues().keySet().stream().toList());

      // If Delta is 1 no suppression (Domain Bounding) is applied
      //if (this.delta != 1) {
        this.stableDomainReached = this.addToDomain(currentItem);

        // If stable domain is reached
        if (this.stableDomainReached) {
          break;
        }
      //} else {
      //  this.domain.add(currentItem);
      //}
    }

    if (!this.stableDomainReached && this.delta != 1) {
      return new ArrayList<>();
    }

    this.updateSensitivities();

    List<AnonymizationItem> outputResult = new ArrayList<>();
    for (DocaItem docaItem : this.domain) {
      List<DocaItem> releasedItems = this.doca(docaItem);
      if (!releasedItems.isEmpty()) {
        outputResult.addAll(releasedItems.stream().map(anonItem ->
            new AnonymizationItem(anonItem.getExternalId(), anonItem.getData(),
                anonItem.getNonAnonymizedData())).toList());
      }
    }
    this.domain = new ArrayList<>();
    return outputResult;
  }


  /**
   * Adds a Tuple to the domain and checks if it is stable.
   * if domain is stable, it will be released
   *
   * @param x Tuple to be added
   * @return the domain if it is stable, otherwise null
   */
  protected Boolean addToDomain(DocaItem x) {
    double tolerance = this.LAMBDA;

    // EXPERIMENTAL: Use mean of tuple as value for GK
    // Get mean value of data point
    double sum = x.getData().values().stream().reduce(0.0, Double::sum);
    double mean = sum / x.getHeaders().size();

    // Add tuple to GKQEstimator
    GKQuantileEstimator.add(mean, x);
    sortedCurrentDomain.put(x, mean);

    // Add Quantile to Vinf and Vsup
    // Key is index, value is value
    HashMap<DocaItem, Double> estQuantileInf =
        GKQuantileEstimator.getQuantile((1.0 - this.delta) / 2.0);
    HashMap<DocaItem, Double> estQuantileSup =
        GKQuantileEstimator.getQuantile((1.0 + this.delta) / 2.0);

    Vinf.add((double) estQuantileInf.values().toArray()[0]);
    Vsup.add((double) estQuantileSup.values().toArray()[0]);

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

        DocaItem from = (DocaItem) estQuantileInf.keySet().toArray()[0];
        DocaItem to = (DocaItem) estQuantileSup.keySet().toArray()[0];

        List<DocaItem> sortedItems = sortedCurrentDomain.entrySet()
            .stream()
            .sorted(Map.Entry.comparingByValue())
            .map(Map.Entry::getKey)
            .toList();
        this.domain = sortedItems.subList(sortedItems.indexOf(from), sortedItems.indexOf(to) + 1);

        Vinf = new ArrayList<>();
        Vsup = new ArrayList<>();
        GKQuantileEstimator = new GreenwaldKhannaQuantileEstimator(0.02);
        sortedCurrentDomain = new HashMap<>();

        // Reset Doca Stage when using StableDomains
        this.clusterList =  new ArrayList<>();
        this.rangeMap = new HashMap<>();
        this.tau = 0;

        return true;
      }
      // remove oldest tuple to remain size of stable domain
      Vinf.remove(0);
      Vsup.remove(0);
    }
    return false;
  }


  /**
   * Adds a Tuple to the domain and checks if it is stable.
   * if domain is stable, it will be released
   *
   * @param dataTuple     Tuple to be added
   * @return the domain if it is stable, otherwise null
   */
  protected List<DocaItem> doca(DocaItem dataTuple) {
    // Add tuple to best cluster and return expired clusters if any
    DocaCluster expiringCluster = this.onlineClustering(dataTuple);

    List<DocaItem> releasedItems = new ArrayList<>();
    // Release Cluster if expired
    if (expiringCluster != null) {
      releasedItems.addAll(releaseExpiredCluster(expiringCluster));
    }
    return releasedItems;
  }

  /**
   * Online Clustering Algorithm.
   *
   * @param tuple Data Tuple to be clustered
   * @return List of expired clusters, the list is empty if no cluster expired
   */
  private DocaCluster onlineClustering(DocaItem tuple) {

    DocaCluster bestCluster = this.findBestCluster(tuple);

    if (bestCluster == null) {
      DocaCluster newCluster = new DocaCluster(tuple.getHeaders(), this.headerWeights);
      newCluster.insert(tuple);
      this.clusterList.add(newCluster);
    } else {
      bestCluster.insert(tuple);
    }

    // update global ranges
    for (String header : tuple.getHeaders()) {
      if (this.rangeMap.containsKey(header)) {
        this.rangeMap.put(header, Utils.updateDoubleRange(this.rangeMap.get(header), tuple.getData().get(header)));
      } else {
        this.rangeMap.put(header, Range.is(tuple.getData().get(header)));
      }
    }

    DocaCluster expiredCluster;
    // add tuple to currently active items
    if (this.currentItems.size() <= this.delayConstraint) {
      this.currentItems.add(tuple);
      return null;
    } else {
      DocaItem expiredTuple = this.currentItems.remove(0);
      this.currentItems.add(tuple);
      expiredCluster = expiredTuple.getCluster();
    }
    return expiredCluster;
  }

  /**
   * Find the best cluster for a given data point.
   *
   * @param dataPoint Data point to be clustered
   * @return Best cluster for the data point or null if no fitting cluster was found
   */
  private DocaCluster findBestCluster(DocaItem dataPoint) {
    int bestCluster = -1;
    List<DocaCluster> clusters = this.clusterList;

    // Calculate enlargement (the value is not yet divided by the number of attributes!)
    List<Double> enlargement = new ArrayList<>();
    for (DocaCluster cluster : clusters) {
      double sum = 0;
      for (Map.Entry<String, Double> entry : dataPoint.getData().entrySet()) {
        String key = entry.getKey();
        Double value = entry.getValue();
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

        // Function tupleEnlargment calculates Loss (which is a bit confusing)
        double overallLoss = clusters.get(c).tupleEnlargement(dataPoint, this.rangeMap);
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
      // If no new cluster is allowed, try to find a cluster with minimum enlargement
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
   * @return an expired cluster or empty list if no cluster was released (e.g. delay constraint not
   *        reached)
   */
  private List<DocaItem> releaseExpiredCluster(DocaCluster expiredCluster) {
    // update values
    double loss = expiredCluster.informationLoss(this.rangeMap);
    this.losses.add(loss);
    //TODO: tau should only be calculated from the last m losses
    this.tau = this.losses.subList(Math.max(losses.size() - 80, 0), losses.size()).stream().mapToDouble(Double::doubleValue).sum() / Math.min(losses.size(), 80);
    // release cluster from list
    this.clusterList.remove(expiredCluster);

    // perturbs cluster items
    HashMap<String, Double> attr = new HashMap<>();
    for (DocaItem item : expiredCluster.getContents()) {
      // Sum up each Attribute
      for (String header : item.getHeaders()) {
        if (!attr.containsKey(header)) {
          attr.put(header, 0.0);
        }
        attr.put(header, attr.get(header) + item.getData().get(header));
      }
    }
    // Calculate mean of Attributes
    HashMap<String, Double> mean = new HashMap<>();
    for (Map.Entry<String, Double> attrEntry : attr.entrySet()) {
      mean.put(attrEntry.getKey(), attrEntry.getValue() / expiredCluster.getContents().size());
    }

    Map<String, Double> noise = getNoise(expiredCluster, this.sensitivityList, mean);

    for (DocaItem i : expiredCluster.getContents()) {
      for (Map.Entry<String, Double> entry : i.getData().entrySet()) {
        entry.setValue(mean.get(entry.getKey()));
      }
    }

    for (DocaItem expired : expiredCluster.getContents()) {
      this.currentItems.removeIf(
          currentItem -> expired.getExternalId().equals(currentItem.getExternalId()));
    }
    expiredCluster.pertubeCluster(noise);
    return expiredCluster.getContents();
  }

  private Map<String, Double> getNoise(DocaCluster expiredCluster,
                                       Map<String, Double> sensitivityMap,
                                       HashMap<String, Double> mean) {
    HashMap<String, Double> laplaceNoise = new HashMap<>();
    for (String header : mean.keySet()) {

      double deltaC = sensitivityMap.get(header) / (expiredCluster.getContents().size());
      double scale = deltaC / eps;

      JDKRandomGenerator rg = new JDKRandomGenerator();
      LaplaceDistribution laplaceDistribution = new LaplaceDistribution(rg, 0, scale);
      Double noise = laplaceDistribution.sample();
      laplaceNoise.put(header, noise);
    }

    return laplaceNoise;
  }

  private void updateSensitivities() {
    Map<String, Double> maximums = DocaUtil.getMax(this.domain);
    Map<String, Double> minimums = DocaUtil.getMin(this.domain);

    for (String header : maximums.keySet()) {
      if (Objects.equals(maximums.get(header), minimums.get(header))) {
        this.sensitivityList.put(header, 1.0);
      } else {
        this.sensitivityList.put(header, Math.abs(minimums.get(header) - maximums.get(header)));
      }
    }
  }


  public double[][] docaAlternative(double[][] x) {
    String[] parameters = getParameters();
    double eps = Double.parseDouble(parameters[0]);
    int delayConstraint = Integer.parseInt(parameters[1]);
    int beta = Integer.parseInt(parameters[2]);
    boolean inplace = Boolean.parseBoolean(parameters[3]);
    int numInstances = x.length;
    int numAttributes = x[0].length;

    //TODO: This could be problematic when more headers with different value ranges are used
    //double sensitivity = Math.abs((DocaUtil.getMax(x) - DocaUtil.getMin(x)));
    double sensitivity = 1.0;

    List<List<Integer>> clusters = new ArrayList<>();
    List<List<Integer>> clustersFinal = new ArrayList<>();

    // Cluster attribute minimum/maximum
    List<double[]> mnC = new ArrayList<>();
    List<double[]> mxC = new ArrayList<>();

    // Global Attribute Minimum/Maximum
    double[] mn = new double[numAttributes];
    double[] mx = new double[numAttributes];
    for (int i = 0; i < numAttributes; i++) {
      mn[i] = Double.POSITIVE_INFINITY;
      mx[i] = Double.NEGATIVE_INFINITY;
    }
    // TODO: Tau should be calculated as mean of the last m losses
    // Losses saved for tau
    List<Double> losses = new ArrayList<>();
    double tau = 0;

    // Create Output structure
    double[][] output;
    if (inplace) {
      output = x;
    } else {
      output = new double[numInstances][numAttributes];
    }

    int TODOREMOVE_Perfect = 0;

    for (int clock = 0; clock < numInstances; clock++) {
      if (clock % 1000 == 0) {
        System.out.println("Clock " + clock + " " + TODOREMOVE_Perfect);
      }

      double[] dataPoint = x[clock];

      // Update min/max
      for (int i = 0; i < numAttributes; i++) {
        mn[i] = Math.min(mn[i], dataPoint[i]);
        mx[i] = Math.max(mx[i], dataPoint[i]);
      }

      double[] dif = new double[numAttributes];
      for (int i = 0; i < numAttributes; i++) {
        dif[i] = mx[i] - mn[i];
      }

      // Find best Cluster
      Integer bestCluster = null;
      if (!clusters.isEmpty()) {
        // Calculate enlargement (the value is not yet divided by the number of attributes!)
        double[] enlargement = new double[clusters.size()];
        for (int c = 0; c < clusters.size(); c++) {
          double sum = 0;
          for (int i = 0; i < numAttributes; i++) {
            sum +=
                Math.max(0, dataPoint[i] - mxC.get(c)[i])
                    - Math.min(0, dataPoint[i] - mnC.get(c)[i]);
          }
          enlargement[c] = sum;
        }

        double minEnlarge = Double.MAX_VALUE;

        List<Integer> okClusters = new ArrayList<>();
        List<Integer> minClusters = new ArrayList<>();

        for (int c = 0; c < clusters.size(); c++) {
          double enl = enlargement[c];
          if (enl == minEnlarge) {
            minClusters.add(c);
            // TODO: Does enl need to be added here?
            double overallLoss = (enl + DocaUtil.getSumOfElementsInArray(DocaUtil.divisionWith0(mxC.get(c), dif)))
                / numAttributes;
            if (overallLoss <= tau) {
              okClusters.add(c);
            }
          }
        }

        if (!okClusters.isEmpty()) {
          TODOREMOVE_Perfect += 1;
          bestCluster =
              okClusters.stream()
                  .min(
                      (c1, c2) -> Integer.compare(clusters.get(c1).size(), clusters.get(c2).size()))
                  .orElse(null);
        } else if (clusters.size() >= beta) {
          bestCluster =
              minClusters.stream()
                  .min(
                      (c1, c2) -> Integer.compare(clusters.get(c1).size(), clusters.get(c2).size()))
                  .orElse(null);
        }
      }

      if (bestCluster == null) {
        // Add new Cluster
        List<Integer> newCluster = new ArrayList<>();
        newCluster.add(clock);
        clusters.add(newCluster);

        // Set Min/Max of new Cluster
        mnC.add(dataPoint.clone());
        mxC.add(dataPoint.clone());
      } else {
        clusters.get(bestCluster).add(clock);
        // Update min/max
        double[] mnCluster = mnC.get(bestCluster);
        double[] mxCluster = mxC.get(bestCluster);
        mnCluster[bestCluster] = Math.min(mnCluster[bestCluster], dataPoint[bestCluster]);
        mxCluster[bestCluster] = Math.max(mxCluster[bestCluster], dataPoint[bestCluster]);
      }

      List<Integer> overripeClusters = new ArrayList<>();
      for (int c = 0; c < clusters.size(); c++) {
        if (clusters.get(c).contains(clock - delayConstraint)) {
          overripeClusters.add(c);
        }
      }
      assert overripeClusters.size() <= 1
          : "Every datapoint should only be able to be in one cluster!?";
      if (!overripeClusters.isEmpty()) {
        int c = overripeClusters.get(0);
        double[] difCluster = new double[numAttributes];
        for (int i = 0; i < numAttributes; i++) {
          difCluster[i] = mxC.get(c)[i] - mnC.get(c)[i];
        }
        double loss = DocaUtil.getSumOfElementsInArray(DocaUtil.divisionWith0(difCluster, dif))
            / numAttributes;
        losses.add(loss);
        clustersFinal.add(clusters.get(c));
        clusters.remove(c);
        mnC.remove(c);
        mxC.remove(c);
      }
    }

    clustersFinal.addAll(clusters);

    for (List<Integer> cs : clustersFinal) {
      double[] mean = new double[numAttributes];
      for (int i : cs) {
        for (int j = 0; j < numAttributes; j++) {
          mean[j] += x[i][j];
        }
      }
      for (int j = 0; j < numAttributes; j++) {
        mean[j] /= cs.size();
      }
      double scale = (sensitivity / (cs.size() * eps));
      double[] laplace = new double[numAttributes];
      for (int j = 0; j < numAttributes; j++) {
        laplace[j] = Math.random() - 0.5;
      }
      for (int j = 0; j < numAttributes; j++) {
        output[cs.get(0)][j] = mean[j] + scale * laplace[j];
      }
    }

    return output;
  }

}
