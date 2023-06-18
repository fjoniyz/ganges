package com.ganges.lib.castleguard;

import com.ganges.lib.castleguard.utils.ClusterManagement;
import com.ganges.lib.castleguard.utils.LogUtils;
import com.ganges.lib.castleguard.utils.Utils;
import java.util.*;
import org.apache.commons.lang3.Range;
import org.apache.commons.math3.distribution.LaplaceDistribution;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CastleGuard {
  private final CGConfig config;
  private List<String> headers;
  private String sensitiveAttr;
  private Deque<Item> items = new ArrayDeque<>(); // a.k.a. global_tuples in castle.py
  private HashMap<String, Range<Float>> globalRanges = new HashMap<>();
  private double tau = Double.POSITIVE_INFINITY;
  private ClusterManagement clusterManagement;
  private Deque<Item> outputQueue = new ArrayDeque<>();

  private final Logger logger = LoggerFactory.getLogger(CastleGuard.class);

  public CastleGuard(CGConfig config, List<String> headers, String sensitiveAttr) {
    this.config = config;
    this.headers = headers;
    this.sensitiveAttr = sensitiveAttr;
    for (String header : headers) {
      globalRanges.put(header, null);
    }
    this.clusterManagement =
        new ClusterManagement(
            this.config.getK(), this.config.getL(), this.config.getMu(), headers, sensitiveAttr);
  }

  public Optional<HashMap<String, Float>> tryGetOutputLine() {
    if (outputQueue.isEmpty()) {
      return Optional.empty();
    }
    Item output = outputQueue.pop();
    return Optional.of(output.getData());
  }

  /**
   * Inserts a new piece of data into the algorithm and updates the state, checking whether data
   * needs to be output as well
   *
   * @param data: The element of data to insert into the algorithm
   */
  public void insertData(HashMap<String, Float> data) {
    Random rand = new Random();
    if (config.isUseDiffPrivacy() && rand.nextDouble() > config.getBigBeta()) {
      logger.info("Suppressing the item");
      return;
    }
    Item item = new Item(data, this.headers, this.sensitiveAttr);
    updateGlobalRanges(item);

    if (config.isUseDiffPrivacy()) {
      perturb(item);
    }
    Optional<Cluster> cluster = bestSelection(item);
    if (!cluster.isPresent()) {
      // Create new cluster
      Cluster newCluster = new Cluster(this.headers);
      this.clusterManagement.addToNonAnonymizedClusters(newCluster);
      newCluster.insert(item);
    } else {
      cluster.get().insert(item);
    }

    items.add(item);
    if (items.size() > config.getDelta()) {
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
    Set<Float> outputPids = new HashSet<>();
    Set<Float> outputDiversity = new HashSet<>();

    boolean splittable =
        cluster.getSize() >= 2 * config.getK() && cluster.getDiversitySize() >= config.getL();
    List<Cluster> splitted =
        splittable
            ? this.clusterManagement.splitL(cluster, this.headers, this.globalRanges)
            : List.of(cluster);
    Iterator<Cluster> clusterIterator = splitted.iterator();
    List<Item> itemsToSuppress = new ArrayList<>();
    while (clusterIterator.hasNext()) {
      Cluster sCluster = clusterIterator.next();
      Iterator<Item> itemIterator = sCluster.getContents().iterator();
      while (itemIterator.hasNext()) {
        Item item = itemIterator.next();
        Item generalized = sCluster.generalise(item);
        outputItem(generalized);

        outputPids.add(item.getData().get("pid"));
        outputDiversity.add(item.getSensitiveAttr());
        itemsToSuppress.add(item);
      }
      itemsToSuppress.forEach(this::suppressItem);

      // Calculate loss
      this.clusterManagement.updateLoss(sCluster, globalRanges);

      assert outputPids.size() >= config.getK();
      assert outputDiversity.size() >= config.getL();

      this.clusterManagement.addToAnonymizedClusters(cluster);
    }
  }

  /**
   * Decides whether to suppress <item> or not
   *
   * @param item : The tuple to make decisions based on
   */
  private void delayConstraint(@NonNull Item item) {
    List<Cluster> nonAnonClusters = this.clusterManagement.getNonAnonymizedClusters();
    List<Cluster> anonClusters = this.clusterManagement.getAnonymizedClusters();

    Cluster itemCluster = item.getCluster();
    if (this.config.getK() <= itemCluster.getSize()
        && this.config.getL() < itemCluster.getDiversitySize()) {
      checkAndOutputCluster(itemCluster);
      return;
    }

    Optional<Cluster> randomCluster =
        anonClusters.stream().filter(c -> c.withinBounds(item)).findAny();
    if (randomCluster.isPresent()) {
      Item generalised = randomCluster.get().generalise(item);
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

  private void outputItem(Item item) {
    outputQueue.push(item);
  }
  /**
   * Suppresses a tuple from being output and deletes it from the CASTLE state. Removes it from the
   * global tuple queue and also the cluster it is being contained in
   *
   * @param item: The tuple to suppress
   */
  public void suppressItem(Item item) {
    List<Cluster> nonAnonClusters = this.clusterManagement.getNonAnonymizedClusters();
    this.items.remove(item);
    Cluster parentCluster = item.getCluster();
    parentCluster.remove(item);

    if (parentCluster.getSize() == 0) {
      nonAnonClusters.remove(parentCluster);
    }
  }

  /**
   * Fudges a tuple based on laplace distribution
   *
   * @param item: The tuple to be perturbed
   */
  private void perturb(Item item) {
    HashMap<String, Float> data = item.getData();

    for (String header : this.headers) {
      // Check if header has a range
      if (this.globalRanges.get(header).getMinimum() != null
          || this.globalRanges.get(header).getMaximum() != null) {
        double max_value = this.globalRanges.get(header).getMaximum();
        double min_value = this.globalRanges.get(header).getMinimum();

        // Calaculate scale
        double scale = Math.max((max_value - min_value), 1) / this.config.getPhi();

        // Draw random noise from Laplace distribution
        JDKRandomGenerator rg = new JDKRandomGenerator();
        LaplaceDistribution laplaceDistribution = new LaplaceDistribution(rg, 0, scale);
        float noise = (float) laplaceDistribution.sample();

        // Add noise to original value
        float originalValue = data.get(header);
        float perturbedValue = originalValue + noise;
        item.updateAttributes(header, perturbedValue);
      }
    }
  }

  /**
   * Finds the best matching cluster for <element>
   *
   * @param item: The tuple to find the best cluster for
   * @return Either a cluster for item to be inserted into, or null if a new cluster should be
   *     created
   */
  private Optional<Cluster> bestSelection(Item item) {
    List<Cluster> notAnonClusters = this.clusterManagement.getNonAnonymizedClusters();

    // Need to be tested

    Set<Float> e = new HashSet<>();

    for (Cluster cluster : notAnonClusters) {
      e.add(cluster.tupleEnlargement(item, globalRanges));
    }

    if (e.isEmpty()) {
      return Optional.empty();
    }

    float minima = Collections.min(e);

    List<Cluster> setCmin = new ArrayList<>();

    for (Cluster cluster : notAnonClusters) {
      if (cluster.tupleEnlargement(item, globalRanges) == minima) {
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
      if (this.config.getBeta() <= notAnonClusters.size()) {
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

  void updateGlobalRanges(Item item) {
    for (Map.Entry<String, Float> header : item.getData().entrySet()) {
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
