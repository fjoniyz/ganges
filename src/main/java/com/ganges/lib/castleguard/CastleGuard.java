package com.ganges.lib.castleguard;

import com.ganges.lib.AbstractItem;
import com.ganges.lib.AnonymizationAlgorithm;
import com.ganges.lib.AnonymizationItem;
import com.ganges.lib.castleguard.utils.ClusterManagement;
import com.ganges.lib.castleguard.utils.LogUtils;
import com.ganges.lib.castleguard.utils.Utils;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.Range;
import org.apache.commons.math3.distribution.LaplaceDistribution;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CastleGuard implements AnonymizationAlgorithm {

  private final Logger logger = LoggerFactory.getLogger(CastleGuard.class);
  private final List<String> headers;
  private final String sensitiveAttr;
  private final Deque<CGItem> items = new ArrayDeque<>(); // a.k.a. global_tuples in castle.py
  private final HashMap<String, Range<Double>> globalRanges = new HashMap<>();
  private final double tau = Double.POSITIVE_INFINITY;
  private final int delta;
  private final int beta;
  private final int bigBeta;
  private final double phi;
  private final int k;
  private final int l;
  private final boolean useDiffPrivacy;
  private final ClusterManagement clusterManagement;
  private final Deque<CGItem> outputQueue = new ArrayDeque<>();
  private Map<String, Double> headerWeights;

  /**
   * !Deprecated constructor
   */
  public CastleGuard(CGConfig config, List<String> headers, String sensitiveAttr) {
    String[] parameters = getParameters();
    this.k = Integer.parseInt(parameters[0]);
    this.delta = Integer.parseInt(parameters[1]);
    this.beta = Integer.parseInt(parameters[2]);
    this.bigBeta = Integer.parseInt(parameters[3]);
    int mu = Integer.parseInt(parameters[4]);
    this.l = Integer.parseInt(parameters[5]);
    this.phi = Double.parseDouble(parameters[6]);
    this.useDiffPrivacy = Boolean.parseBoolean(parameters[7]);
    this.headers = Arrays.asList(parameters[8].split(","));
    this.sensitiveAttr = parameters[9];

    for (String header : this.headers) {
      globalRanges.put(header, null);
    }
    this.clusterManagement =
        new ClusterManagement(
            this.k, this.l, mu, this.headers, this.sensitiveAttr);
  }


  public CastleGuard() {
    String[] parameters = getParameters();
    this.k = Integer.parseInt(parameters[0]);
    this.delta = Integer.parseInt(parameters[1]);
    this.beta = Integer.parseInt(parameters[2]);
    this.bigBeta = Integer.parseInt(parameters[3]);
    int mu = Integer.parseInt(parameters[4]);
    this.l = Integer.parseInt(parameters[5]);
    this.phi = Double.parseDouble(parameters[6]);
    this.useDiffPrivacy = Boolean.parseBoolean(parameters[7]);
    this.headers = Arrays.asList(parameters[8].split(","));
    this.sensitiveAttr = parameters[9];

    for (String header : this.headers) {
      globalRanges.put(header, null);
    }
    this.clusterManagement =
        new ClusterManagement(
            this.k, this.l, mu, this.headers, this.sensitiveAttr);
  }

  public static String[] getParameters() {
    String[] result = new String[10];
    String userDirectory = System.getProperty("user.dir");
    try (InputStream inputStream = Files.newInputStream(
        Paths.get(userDirectory + "/src/main/resources/castleguard.properties"))) {
      Properties properties = new Properties();
      properties.load(inputStream);
      result[0] = properties.getProperty("k");
      result[1] = properties.getProperty("delta");
      result[2] = properties.getProperty("beta");
      result[3] = properties.getProperty("bigBeta");
      result[4] = properties.getProperty("mu");
      result[5] = properties.getProperty("l");
      result[6] = properties.getProperty("phi");
      result[7] = properties.getProperty("useDiffPrivacy");
      result[8] = properties.getProperty("headers");
      result[9] = properties.getProperty("sensitive_attribute");
      return result;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public HashMap<String, Range<Double>> getGlobalRanges() {
    return globalRanges;
  }

  public ClusterManagement getClusterManagement() {
    return clusterManagement;
  }

  public Deque<CGItem> getItems() {
    return items;
  }

  public Optional<CGItem> tryGetOutputLine() {
    if (outputQueue.isEmpty()) {
      return Optional.empty();
    }
    CGItem output = outputQueue.pop();
    return Optional.of(output);
  }

  @Override
  public List<AnonymizationItem> anonymize(List<AnonymizationItem> X) {
    this.headerWeights = X.get(0).getHeaderWeights();
    for (AnonymizationItem dataPoint : X) {
      CGItem item = new CGItem(dataPoint.getId(), dataPoint.getValues(), dataPoint.getNonAnonymizedValues(),
          this.headers,
          this.sensitiveAttr);
      insertData(item);
    }
    List<AnonymizationItem> outputItems = outputQueue.stream().map(cgItem -> {
          //TODO: Unify value data type to avoid this mess
          Map<String, Double> values = cgItem.getData().entrySet().stream()
              .collect(Collectors.toMap(Map.Entry::getKey,
                  Map.Entry::getValue));
          return new AnonymizationItem(cgItem.getExternalId(), values, cgItem.getNonAnonymizedData());
        }
    ).toList();
    outputQueue.clear();

    // Filter out median of cluster values. TODO: Check if we only need this median, or if we want min, max or spc as well
    List<AnonymizationItem> updatedOutput = outputItems.stream()
        .map(item -> {
          Map<String, Double> hashMap = item.getValues();
          Map<String, Double> newHashMap = new HashMap<>();
          for (String header : headers) {
            double headerValue = hashMap.getOrDefault(header, 0.0);
            newHashMap.put(header, headerValue); // Create a new HashMap with only the "Header" key
          }
          return new AnonymizationItem(item.getId(), newHashMap, item.getNonAnonymizedValues());
        })
        .collect(Collectors.toList());

    return updatedOutput;
  }

  /**
   * Inserts a new piece of data into the algorithm and updates the state, checking whether data
   * needs to be output as well
   *
   * @param data: The element of data to insert into the algorithm
   */
  public void insertData(HashMap<String, Double> data) {
    CGItem item = new CGItem("", data, new HashMap<>(), this.headers, this.sensitiveAttr);
    insertData(item);
  }

  public void insertData(CGItem item) {
    Random rand = new Random();
    if (this.useDiffPrivacy && rand.nextDouble() > this.bigBeta) {
      logger.info("Suppressing the item");
      return;
    }

    updateGlobalRanges(item);

    if (this.useDiffPrivacy) {
      perturb(item);
    }
    Optional<Cluster> cluster = bestSelection(item);
    if (!cluster.isPresent()) {
      // Create new cluster
      Cluster newCluster = new Cluster(this.headers, this.headerWeights);
      this.clusterManagement.addToNonAnonymizedClusters(newCluster);
      newCluster.insert(item);
    } else {
      cluster.get().insert(item);
    }

    items.add(item);
    if (items.size() > this.delta) {
      delayConstraint(items.pop());
    }
    this.clusterManagement.updateTau(this.globalRanges);
  }

  /**
   * Previously outputCluster
   *
   * @param cluster
   */
  public void checkAndOutputCluster(Cluster cluster) {
    Set<Double> outputPids = new HashSet<>();
    Set<Double> outputDiversity = new HashSet<>();
    boolean splittable =
        cluster.getKSize() >= 2 * this.k && cluster.getDiversitySize() >= this.l;
    List<Cluster> splitted =
        splittable
            ? this.clusterManagement.splitL(cluster, this.headers, this.globalRanges)
            : List.of(cluster);
    Iterator<Cluster> clusterIterator = splitted.iterator();
    List<CGItem> itemsToSuppress = new ArrayList<>();
    while (clusterIterator.hasNext()) {
      Cluster sCluster = clusterIterator.next();
      Iterator<CGItem> itemIterator = sCluster.getContents().iterator();
      while (itemIterator.hasNext()) {
        CGItem item = itemIterator.next();
        CGItem generalized = sCluster.generalise(item);
        outputItem(generalized);

        outputPids.add(item.getData().get("pid"));
        outputDiversity.add(item.getSensitiveAttr());
        itemsToSuppress.add(item);
      }
      itemsToSuppress.forEach(this::suppressItem);

      // Calculate loss
      this.clusterManagement.updateLoss(sCluster, globalRanges);

      assert outputPids.size() >= this.k;
      assert outputDiversity.size() >= this.l;

      this.clusterManagement.addToAnonymizedClusters(cluster);
      this.clusterManagement.removeFromNonAnonymizedClusters(cluster);
    }
  }

  /**
   * Decides whether to suppress <item> or not
   *
   * @param item : The tuple to make decisions based on
   */
  private void delayConstraint(@NonNull CGItem item) {
    List<Cluster> nonAnonClusters = this.clusterManagement.getNonAnonymizedClusters();
    List<Cluster> anonClusters = this.clusterManagement.getAnonymizedClusters();

    Cluster itemCluster = item.getCluster();
    if (this.k <= itemCluster.getSize()
        && this.l < itemCluster.getDiversitySize()) {
      checkAndOutputCluster(itemCluster);
      return;
    }

    Optional<Cluster> randomCluster =
        anonClusters.stream().filter(c -> c.withinBounds(item)).findAny();
    if (randomCluster.isPresent()) {
      CGItem generalised = randomCluster.get().generalise(item);
      suppressItem(item);
      outputItem(generalised);
      return;
    }

    int biggerClustersNum = 0;
    for (Cluster cluster : nonAnonClusters) {
      if (itemCluster.getSize() < cluster.getSize()) {
        biggerClustersNum++;
      }
    }
    if (biggerClustersNum > nonAnonClusters.size() / 2) {
      suppressItem(item);
      return;
    }
    Cluster merged = this.clusterManagement.mergeClusters(itemCluster, globalRanges);
    checkAndOutputCluster(merged);
  }

  private void outputItem(CGItem item) {
    outputQueue.push(item);
  }

  /**
   * Suppresses a tuple from being output and deletes it from the CASTLE state. Removes it from the
   * global tuple queue and also the cluster it is being contained in
   *
   * @param item: The tuple to suppress
   */
  public void suppressItem(CGItem item) {
    if (this.items.contains(item)) {
      List<Cluster> nonAnonClusters = this.clusterManagement.getNonAnonymizedClusters();
      this.items.remove(item);
      Cluster parentCluster = item.getCluster();
      parentCluster.remove(item);

      if (parentCluster.getSize() == 0) {
        nonAnonClusters.remove(parentCluster);
      }
    }
  }

  /**
   * Fudges a tuple based on laplace distribution
   *
   * @param item: The tuple to be perturbed
   */
  private void perturb(CGItem item) {
    Map<String, Double> data = item.getData();

    for (String header : this.headers) {
      // Check if header has a range
      if (this.globalRanges.get(header).getMinimum() != null
          || this.globalRanges.get(header).getMaximum() != null) {
        double max_value = this.globalRanges.get(header).getMaximum();
        double min_value = this.globalRanges.get(header).getMinimum();

        // Calaculate scale
        double scale = Math.max((max_value - min_value), 1) / this.phi;

        // Draw random noise from Laplace distribution
        JDKRandomGenerator rg = new JDKRandomGenerator();
        LaplaceDistribution laplaceDistribution = new LaplaceDistribution(rg, 0, scale);
        Double noise = (Double) laplaceDistribution.sample();

        // Add noise to original value
        Double originalValue = data.get(header);
        Double perturbedValue = originalValue + noise;
        item.updateAttributes(header, perturbedValue);
      }
    }
  }

  /**
   * Finds the best matching cluster for <element>
   *
   * @param item: The tuple to find the best cluster for
   * @return Either a cluster for item to be inserted into, or null if a new cluster should be
   * created
   */
  private Optional<Cluster> bestSelection(CGItem item) {
    List<Cluster> notAnonClusters = this.clusterManagement.getNonAnonymizedClusters();

    // Need to be tested

    Set<Double> e = new HashSet<>();

    for (Cluster cluster : notAnonClusters) {
      e.add(cluster.tupleEnlargement(item, globalRanges));
    }

    if (e.isEmpty()) {
      return Optional.empty();
    }

    Double minima = Collections.min(e);

    List<Cluster> setCmin = new ArrayList<>();

    for (Cluster cluster : notAnonClusters) {
      Double enl = cluster.tupleEnlargement(item, globalRanges);
      if (enl.equals(minima)) {
        setCmin.add(cluster);
      }
    }

    Set<Cluster> setCok = new HashSet<>();

    for (Cluster cluster : setCmin) {
      double ilcj = cluster.informationLossGivenT(item, globalRanges);
      if (ilcj <= tau) {
        setCok.add(cluster);
      }
    }

    if (setCok.isEmpty()) {
      if (this.beta <= notAnonClusters.size()) {
        Random rand = new Random();
        int randomIndex = rand.nextInt(setCmin.size());
        List<Cluster> setCminList = new ArrayList<>(setCmin);
        return Optional.of(setCminList.get(randomIndex));
      }

      return Optional.empty();
    }

    List<Cluster> setCokList = new ArrayList<>(setCok);
    Random rand = new Random();
    int randomIndex = rand.nextInt(setCokList.size());
    return Optional.of(setCokList.get(randomIndex));
  }

  void updateGlobalRanges(CGItem item) {
    for (Map.Entry<String, Double> header : item.getData().entrySet()) {
      if (this.globalRanges.get(header.getKey()) == null) {
        this.globalRanges.put(header.getKey(), Range.is(header.getValue()));
      } else {
        this.globalRanges.put(
            header.getKey(),
            Utils.updateRange(this.globalRanges.get(header.getKey()), header.getValue()));
      }
    }

    LogUtils.logGlobalRanges(globalRanges);
    // globalRanges.replaceAll(
    //        (h, v) -> Utils.updateRange(globalRanges.get(h), item.getData().get(h)));
  }


}