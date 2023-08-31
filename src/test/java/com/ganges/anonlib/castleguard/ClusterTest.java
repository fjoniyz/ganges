package com.ganges.anonlib.castleguard;

import org.junit.Assert;
import org.junit.Test;
import org.apache.commons.lang3.Range;

import java.util.*;

public class ClusterTest {

  /**
   * Creates an item with given parameters
   * @param headers
   * @param timeseriesId
   * @param secondsEnergyConsumption
   * @param sensitiveAttribute
   * @return
   */
  public CGItem createItem(List<String> headers, Double timeseriesId,
                           Double secondsEnergyConsumption,
                           String sensitiveAttribute){
    HashMap<String, Double> data = new HashMap<>();
    data.put("timeseries_id", timeseriesId);
    data.put("Seconds_EnergyConsumption", secondsEnergyConsumption);
    return new CGItem("1", data, new HashMap<>(), headers, sensitiveAttribute);
  }

  /**
   * Testing the insertion of an item into the cluster
   */
  @Test
  public void insertTest() {
    // create a cluster
    String[] array = {"timeseries_id", "Seconds_EnergyConsumption"};
    List<String> headers = Arrays.asList(array);
    HashMap<String, Double> weights = new HashMap<>();
    headers.forEach(header -> weights.put(header, 1.0));
    Cluster testCluster = new Cluster(headers, weights);

    CGItem testItem = createItem(headers,1.0, 2.0, "Seconds_EnergyConsumption");

    // create variables with expected values
    Map<String, Range<Double>> expectedRanges = new LinkedHashMap<>();
    headers.forEach(header -> expectedRanges.put(header, Range.between(0.0, 0.0)));
    List<CGItem> expectedContents = new ArrayList<CGItem>();
    expectedContents.add(testItem);
    Set<Double> expectedDiversity = new HashSet<Double>();
    expectedDiversity.add(2.0);

    testCluster.insert(testItem);

    List<CGItem> contents = testCluster.getContents();
    Set<Double> diversity = testCluster.getDiversity();

    Assert.assertEquals(expectedContents, contents);
    Assert.assertEquals(expectedDiversity, diversity);

    // test the ranges
    Map<String, Range<Double>> ranges = testCluster.getRanges();

    for (Map.Entry<String, Range<Double>> header : expectedRanges.entrySet()) {
      header.setValue(
          Range.between(
              testItem.getData().get(header.getKey()), testItem.getData().get(header.getKey())));
    }

    Assert.assertEquals(expectedRanges, ranges);

    // test what happens if we add the same item -> should not be added
    testCluster.insert(testItem);

    Assert.assertEquals(expectedContents, testCluster.getContents());
    Assert.assertEquals(expectedDiversity, testCluster.getDiversity());
    Assert.assertEquals(expectedRanges, testCluster.getRanges());

    // test adding another element to a non-empty cluster

    CGItem testItem2 = createItem(headers, 3.0, 4.0, "Seconds_EnergyConsumption");

    testCluster.insert(testItem2);

    expectedContents.add(testItem2);
    expectedDiversity.add(4.0);

    for (Map.Entry<String, Range<Double>> header : expectedRanges.entrySet()) {
      header.setValue(
          Range.between(
              Math.min(header.getValue().getMinimum(), testItem2.getData().get(header.getKey())),
              Math.max(header.getValue().getMaximum(), testItem2.getData().get(header.getKey()))));
    }

    Assert.assertEquals(expectedContents, testCluster.getContents());
    Assert.assertEquals(expectedDiversity, testCluster.getDiversity());
    Assert.assertEquals(expectedRanges, testCluster.getRanges());
  }

  /**
   * Testing the removing of an item
   */
  @Test
  public void removeTest() {
    // create a cluster
    String[] array = {"timeseries_id", "Seconds_EnergyConsumption"};
    List<String> headers = Arrays.asList(array);
    HashMap<String, Double> weights = new HashMap<>();
    headers.forEach(header -> weights.put(header, 1.0));

    Cluster testCluster = new Cluster(headers, weights);

    CGItem testItem = createItem(headers, 1.0, 2.0, "Seconds_EnergyConsumption");
    CGItem testItem2 = createItem(headers, 5.0, 1.0, "Seconds_EnergyConsumption");
    CGItem testItem3 = createItem(headers, 4.0, 6.0, "Seconds_EnergyConsumption");

    testCluster.insert(testItem);
    testCluster.insert(testItem2);
    testCluster.insert(testItem3);

    // create variables with expected values
    List<CGItem> expectedContents = new ArrayList<CGItem>();
    expectedContents.add(testItem);
    expectedContents.add(testItem2);
    expectedContents.add(testItem3);
    Set<Double> expectedDiversity = new HashSet<Double>();
    expectedDiversity.add(1.0);
    expectedDiversity.add(6.0);

    // check if everything correct after insertion
    Assert.assertEquals(expectedContents, testCluster.getContents());

    testCluster.remove(testItem);
    expectedContents.remove(testItem);

    Assert.assertEquals(expectedContents, testCluster.getContents());
    Assert.assertEquals(expectedDiversity, testCluster.getDiversity());

    // check the ranges
    Map<String, Range<Double>> expectedRanges = new LinkedHashMap<>();
    expectedRanges.put("timeseries_id", Range.between(4.0, 5.0));
    expectedRanges.put("Seconds_EnergyConsumption", Range.between(1.0, 6.0));

    Assert.assertEquals(expectedRanges, testCluster.getRanges());

    // insert the item again and remove another one to check the ranges
    testCluster.insert(testItem);
    testCluster.remove(testItem2);

    expectedRanges.replace("timeseries_id", Range.between(4.0, 5.0),Range.between(1.0, 4.0));
    expectedRanges.replace("Seconds_EnergyConsumption", Range.between(1.0, 6.0), Range.between(2.0,
        6.0));

    Assert.assertEquals(expectedRanges, testCluster.getRanges());

    // check the ranges with a different combination of items
    Cluster testCluster2 = new Cluster(headers, weights);

    testCluster2.insert(testItem);
    testCluster2.insert(testItem2);
    testCluster2.insert(testItem3);

    testCluster2.remove(testItem2);
    Assert.assertEquals(expectedRanges, testCluster2.getRanges());
  }

  CGItem element(Double spcTimeseries, Double spcEngergyConsumption, List<String> headers) {
    HashMap<String, Double> expectedData = new HashMap<>();
    expectedData.put("mintimeseries_id", 1.0);
    expectedData.put("spctimeseries_id", spcTimeseries);
    expectedData.put("maxtimeseries_id", 4.0);
    expectedData.put("minSeconds_EnergyConsumption", 200.0);
    expectedData.put("spcSeconds_EnergyConsumption", spcEngergyConsumption);
    expectedData.put("maxSeconds_EnergyConsumption", 400.0);


    return new CGItem("1", expectedData, new HashMap<>(), headers, null);
  }

  /**
   * Testing the generalisation in the cluster
   */
  @Test
  public void generaliseTest() {
    ArrayList<String> headers = new ArrayList<>();
    headers.add("timeseries_id");
    headers.add("Seconds_EnergyConsumption");
    HashMap<String, Double> weights = new HashMap<>();
    headers.forEach(header -> weights.put(header, 1.0));
    Cluster cluster = new Cluster(headers, weights);

    CGItem one = createItem(headers, 1.0, 200.0, null);
    CGItem two = createItem(headers, 4.0, 400.0, null);

    cluster.insert(one);
    cluster.insert(two);
    CGItem result = cluster.generalise(one);

    String[] genArray = {
      "mintimeseries_id",
      "spctimeseries_id",
      "maxtimeseries_id",
      "minSeconds_EnergyConsumption",
      "spcSeconds_EnergyConsumption",
      "maxSeconds_EnergyConsumption"
    };
    List<String> genHeaders = Arrays.asList(genArray);

    CGItem expectedItemOne = element(1.0, 200.0, genHeaders);
    CGItem expectedItemTwo = element(4.0, 200.0, genHeaders);
    CGItem expectedItemThree = element(1.0, 400.0, genHeaders);
    CGItem expectedItemFour = element(4.0, 400.0, genHeaders);

    Assert.assertTrue(
        result.getData().equals(expectedItemOne.getData())
            || result.getData().equals(expectedItemTwo.getData())
            || result.getData().equals(expectedItemThree.getData())
            || result.getData().equals(expectedItemFour.getData()));
  }

  /**
   * Testing the tuple enlargement
   */
  @Test
  public void tupleEnlargementTest() {
    String[] array = {"timeseries_id", "Seconds_EnergyConsumption"};
    List<String> headers = Arrays.asList(array);
    HashMap<String, Double> weights = new HashMap<>();
    headers.forEach(header -> weights.put(header, 1.0));
    Cluster testCluster = new Cluster(headers, weights);

    CGItem testItem = createItem(headers, 5.0, 300.0, null);
    CGItem testItem2 = createItem(headers, 4.0, 200.0, null);
    CGItem testItem3 = createItem(headers, 2.0, 250.0, null);

    testCluster.insert(testItem);
    testCluster.insert(testItem2);

    HashMap<String, Range<Double>> globalRanges = new HashMap<>();
    globalRanges.put(headers.get(0), Range.between(0.0, 100.0));
    globalRanges.put(headers.get(1), Range.between(0.0, 1000.0));

    Double tupleEnlargementValue = testCluster.tupleEnlargement(testItem3, globalRanges);
    Assert.assertEquals(0.01, tupleEnlargementValue, 0.001);

    // the case when the enlargement is zero
    CGItem testItem4 = createItem(headers, 2.0, 200.0,  null);

    Cluster testCluster2 = new Cluster(headers, weights);
    testCluster2.insert(testItem4);
    testCluster2.insert(testItem);

    Double tupleEnlargementValue2 = testCluster2.tupleEnlargement(testItem3, globalRanges);
    Assert.assertEquals(0, tupleEnlargementValue2, 0.01);
  }

  /**
   * Testing the cluster enlargement
   */
  @Test
  public void clusterEnlargementTest() {
    String[] array = {"timeseries_id", "Seconds_EnergyConsumption"};
    List<String> headers = Arrays.asList(array);
    HashMap<String, Double> weights = new HashMap<>();
    headers.forEach(header -> weights.put(header, 1.0));

    Cluster testCluster = new Cluster(headers, weights);
    Cluster testCluster2 = new Cluster(headers, weights);
    Cluster testCluster3 = new Cluster(headers, weights);
    Cluster testCluster4 = new Cluster(headers, weights);

    HashMap<String, Range<Double>> globalRanges = new HashMap<>();
    globalRanges.put(headers.get(0), Range.between(0.0, 100.0));
    globalRanges.put(headers.get(1), Range.between(0.0, 1000.0));

    CGItem testItem = createItem(headers, 5.0, 300.0,  null);
    CGItem testItem2 = createItem(headers, 4.0, 200.0, null);
    CGItem testItem3 = createItem(headers, 2.0, 250.0, null);
    CGItem testItem4 = createItem(headers, 1.0, 500.0, null);

    testCluster.insert(testItem);
    testCluster.insert(testItem2);

    testCluster2.insert(testItem3);
    testCluster2.insert(testItem4);

//    System.out.println(testCluster.getRanges()); //-> [4;5] , [200;300]
//    System.out.println(testCluster2.getRanges()); //-> [1;2] , [250;500]
//    Expected ranges after merging: [1;5], [200;500]

    Double enlargementValue = testCluster.clusterEnlargement(testCluster2, globalRanges);

    Double expectedValue = 0.23 / 2;
    Assert.assertEquals(expectedValue,enlargementValue,0);

    testCluster3.insert(testItem);
    testCluster3.insert(testItem4);
    testCluster4.insert(testItem2);
    testCluster4.insert(testItem3);

//    System.out.println(testCluster3.getRanges()); //-> [1;5] , [300;500]
//    System.out.println(testCluster4.getRanges()); //-> [2;4] , [200;250]

    Double enlargementValue2 = testCluster3.clusterEnlargement(testCluster4, globalRanges);
    Assert.assertEquals(0.05,enlargementValue2,0.00001);

  }

  /**
   * Testing the information loss with a tuple
   */
  @Test
  public void informationLossGivenTTest() {
    String[] array = {"timeseries_id", "Seconds_EnergyConsumption"};
    List<String> headers = Arrays.asList(array);
    HashMap<String, Double> weights = new HashMap<>();
    headers.forEach(header -> weights.put(header, 1.0));
    Cluster testCluster = new Cluster(headers, weights);

    HashMap<String, Range<Double>> globalRanges = new HashMap<>();
    globalRanges.put(headers.get(0), Range.between(0.0, 100.0));
    globalRanges.put(headers.get(1), Range.between(0.0, 1000.0));

    CGItem one = createItem(headers, 1.0, 200.0, null);
    CGItem two = createItem(headers, 5.0, 300.0, null);
    CGItem three = createItem(headers, 2.0, 250.0, null);

    Double loss1 = testCluster.informationLossGivenT(one, globalRanges);
    Assert.assertEquals(0.0, loss1, 0.0);

    testCluster.insert(one);

    Double loss2 = testCluster.informationLossGivenT(two, globalRanges);
    Assert.assertEquals(0.14, loss2, 0.0);

    testCluster.insert(two);

    Double loss3 = testCluster.informationLossGivenT(three, globalRanges);
    Assert.assertEquals(0.14, loss3, 0.0);
  }

  /**
   * Testing the information loss with a cluster
   */
  @Test
  public void informationLossGivenCTest() {
    String[] array = {"timeseries_id", "Seconds_EnergyConsumption"};
    List<String> headers = Arrays.asList(array);
    HashMap<String, Double> weights = new HashMap<>();
    headers.forEach(header -> weights.put(header, 1.0));
    Cluster testClusterOne = new Cluster(headers, weights);
    Cluster testClusterTwo = new Cluster(headers, weights);

    HashMap<String, Range<Double>> globalRanges = new HashMap<>();
    globalRanges.put(headers.get(0), Range.between(0.0, 100.0));
    globalRanges.put(headers.get(1), Range.between(0.0, 1000.0));

    CGItem one = createItem(headers,1.0, 200.0,  null);
    CGItem two = createItem(headers, 5.0, 300.0, null);
    CGItem three = createItem(headers, 2.0, 250.0, null);
    CGItem four = createItem(headers, 2.0, 100.0, null);

    Double loss0 = testClusterOne.informationLossGivenC(testClusterTwo, globalRanges);
    Assert.assertEquals(0.0, loss0, 0.0);

    testClusterOne.insert(one);
    Double loss1 = testClusterOne.informationLossGivenC(testClusterTwo, globalRanges);
    Assert.assertEquals(0.0, loss1, 0.0);

    testClusterOne.insert(two);
    Double loss2 = testClusterOne.informationLossGivenC(testClusterTwo, globalRanges);
    Assert.assertEquals(0.14, loss2, 0.0);

    testClusterTwo.insert(three);
    Double loss3 = testClusterOne.informationLossGivenC(testClusterTwo, globalRanges);
    Assert.assertEquals(0.14, loss3, 0.0);

    testClusterTwo.insert(four);
    Double loss4 = testClusterOne.informationLossGivenC(testClusterTwo, globalRanges);
    Assert.assertEquals(0.24, loss4, 0.000001);
  }

  /**
   * Testing the information loss
   */
  @Test
  public void informationLossTest() {
    // EV_Usage from 100 to 1000 (according to the data generator)
    // number of overall 100 stations
    // Create a cluster with 3 items inside

    String[] array = {"timeseries_id", "Seconds_EnergyConsumption"};
    List<String> headers = Arrays.asList(array);
    HashMap<String, Double> weights = new HashMap<>();
    headers.forEach(header -> weights.put(header, 1.0));
    Cluster testCluster = new Cluster(headers, weights);

    HashMap<String, Range<Double>> globalRanges = new HashMap<>();
    globalRanges.put(headers.get(0), Range.between(0.0, 100.0));
    globalRanges.put(headers.get(1), Range.between(0.0, 1000.0));

    CGItem one = createItem(headers, 1.0, 200.0, null);
    CGItem two = createItem(headers, 5.0, 300.0, null);
    CGItem three = createItem(headers, 2.0, 250.0, null);

    Double loss0 = testCluster.informationLoss(globalRanges);
    Assert.assertEquals(0.0, loss0, 0.0);

    testCluster.insert(one);
    Double loss1 = testCluster.informationLoss(globalRanges);
    Assert.assertEquals(0.0, loss1, 0.0);

    testCluster.insert(two);
    Double loss2 = testCluster.informationLoss(globalRanges);
    Assert.assertEquals(0.14, loss2, 0.0);

    testCluster.insert(three);
    Double loss3 = testCluster.informationLoss(globalRanges);
    Assert.assertEquals(0.14, loss3, 0.0);
  }

  /**
   * Testing the distance
   */
  @Test
  public void distanceTest() {
    String[] array = {"timeseries_id", "Seconds_EnergyConsumption"};
    List<String> headers = Arrays.asList(array);
    HashMap<String, Double> weights = new HashMap<>();
    headers.forEach(header -> weights.put(header, 1.0));

    CGItem one = createItem(headers, 1.0, 200.0, null);
    CGItem two = createItem(headers, 4.0, 400.0, null);

    Cluster cluster = new Cluster(headers, weights);

    cluster.insert(one);
    cluster.insert(two);

    Double dist = cluster.distance(two);
    Assert.assertEquals(dist, 201.0, 0.0);
    }

  /**
   * Testing the without bounds method
   */
  @Test
  public void withinBoundsTest() {
    String[] array = {"timeseries_id", "Seconds_EnergyConsumption"};
    List<String> headers = Arrays.asList(array);
    HashMap<String, Double> weights = new HashMap<>();
    headers.forEach(header -> weights.put(header, 1.0));

    CGItem one = createItem(headers, 1.0, 200.0, null);
    CGItem two = createItem(headers, 4.0, 400.0, null);

    Cluster cluster = new Cluster(headers, weights);

    Assert.assertFalse(cluster.withinBounds(one));
    cluster.insert(one);
    Assert.assertTrue(cluster.withinBounds(one));
    Assert.assertFalse(cluster.withinBounds(two));
    cluster.insert(two);
    Assert.assertTrue(cluster.withinBounds(two));
  }
}