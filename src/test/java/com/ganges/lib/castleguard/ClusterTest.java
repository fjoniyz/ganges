package com.ganges.lib.castleguard;

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
  public CGItem createItem(List<String> headers, Float timeseriesId, Float secondsEnergyConsumption, String sensitiveAttribute){
    HashMap<String, Float> data = new HashMap<>();
    data.put("timeseries_id", timeseriesId);
    data.put("Seconds_EnergyConsumption", secondsEnergyConsumption);
    return new CGItem(data, headers, sensitiveAttribute);
  }

  /**
   * Testing the insertion of an item into the cluster
   */
  @Test
  public void insertTest() {
    // create a cluster
    String[] array = {"timeseries_id", "Seconds_EnergyConsumption"};
    List<String> headers = Arrays.asList(array);
    Cluster testCluster = new Cluster(headers);

    CGItem testItem = createItem(headers,1.0F, 2.0F, "Seconds_EnergyConsumption");

    // create variables with expected values
    Map<String, Range<Float>> expectedRanges = new LinkedHashMap<>();
    headers.forEach(header -> expectedRanges.put(header, Range.between(0F, 0F)));
    List<CGItem> expectedContents = new ArrayList<CGItem>();
    expectedContents.add(testItem);
    Set<Float> expectedDiversity = new HashSet<Float>();
    expectedDiversity.add(2.0F);

    testCluster.insert(testItem);

    List<CGItem> contents = testCluster.getContents();
    Set<Float> diversity = testCluster.getDiversity();

    Assert.assertEquals(expectedContents, contents);
    Assert.assertEquals(expectedDiversity, diversity);

    // test the ranges
    Map<String, Range<Float>> ranges = testCluster.getRanges();

    for (Map.Entry<String, Range<Float>> header : expectedRanges.entrySet()) {
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

    CGItem testItem2 = createItem(headers, 3.0F, 4.0F, "Seconds_EnergyConsumption");

    testCluster.insert(testItem2);

    expectedContents.add(testItem2);
    expectedDiversity.add(4.0F);

    for (Map.Entry<String, Range<Float>> header : expectedRanges.entrySet()) {
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

    Cluster testCluster = new Cluster(headers);

    CGItem testItem = createItem(headers, 1.0F, 2.0F, "Seconds_EnergyConsumption");
    CGItem testItem2 = createItem(headers, 5.0F, 1.0F, "Seconds_EnergyConsumption");
    CGItem testItem3 = createItem(headers, 4.0F, 6.0F, "Seconds_EnergyConsumption");

    testCluster.insert(testItem);
    testCluster.insert(testItem2);
    testCluster.insert(testItem3);

    // create variables with expected values
    List<CGItem> expectedContents = new ArrayList<CGItem>();
    expectedContents.add(testItem);
    expectedContents.add(testItem2);
    expectedContents.add(testItem3);
    Set<Float> expectedDiversity = new HashSet<Float>();
    expectedDiversity.add(1.0F);
    expectedDiversity.add(6.0F);

    // check if everything correct after insertion
    Assert.assertEquals(expectedContents, testCluster.getContents());

    testCluster.remove(testItem);
    expectedContents.remove(testItem);

    Assert.assertEquals(expectedContents, testCluster.getContents());
    Assert.assertEquals(expectedDiversity, testCluster.getDiversity());

    // check the ranges
    Map<String, Range<Float>> expectedRanges = new LinkedHashMap<>();
    expectedRanges.put("timeseries_id", Range.between(4F, 5F));
    expectedRanges.put("Seconds_EnergyConsumption", Range.between(1F, 6F));

    Assert.assertEquals(expectedRanges, testCluster.getRanges());

    // insert the item again and remove another one to check the ranges
    testCluster.insert(testItem);
    testCluster.remove(testItem2);

    expectedRanges.replace("timeseries_id", Range.between(4F, 5F),Range.between(1F, 4F));
    expectedRanges.replace("Seconds_EnergyConsumption", Range.between(1F, 6F), Range.between(2F, 6F));

    Assert.assertEquals(expectedRanges, testCluster.getRanges());

    // check the ranges with a different combination of items
    Cluster testCluster2 = new Cluster(headers);

    testCluster2.insert(testItem);
    testCluster2.insert(testItem2);
    testCluster2.insert(testItem3);

    testCluster2.remove(testItem2);
    Assert.assertEquals(expectedRanges, testCluster2.getRanges());
  }

  CGItem element(float spcTimeseries, float spcEngergyConsumption, List<String> headers) {
    HashMap<String, Float> expectedData = new HashMap<>();
    expectedData.put("mintimeseries_id", 1.0F);
    expectedData.put("spctimeseries_id", spcTimeseries);
    expectedData.put("maxtimeseries_id", 4.0F);
    expectedData.put("minSeconds_EnergyConsumption", 200.0F);
    expectedData.put("spcSeconds_EnergyConsumption", spcEngergyConsumption);
    expectedData.put("maxSeconds_EnergyConsumption", 400.0F);
    return new CGItem(expectedData, headers, null);
  }

  /**
   * Testing the generalisation in the cluster
   */
  @Test
  public void generaliseTest() {
    ArrayList<String> headers = new ArrayList<>();
    headers.add("timeseries_id");
    headers.add("Seconds_EnergyConsumption");
    Cluster cluster = new Cluster(headers);

    CGItem one = createItem(headers, 1.0F, 200.0F, null);
    CGItem two = createItem(headers, 4.0F, 400.0F, null);

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

    CGItem expectedItemOne = element(1.0F, 200.0F, genHeaders);
    CGItem expectedItemTwo = element(4.0F, 200.0F, genHeaders);
    CGItem expectedItemThree = element(1.0F, 400.0F, genHeaders);
    CGItem expectedItemFour = element(4.0F, 400.0F, genHeaders);

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
    Cluster testCluster = new Cluster(headers);

    CGItem testItem = createItem(headers, 5.0F, 300.0F, null);
    CGItem testItem2 = createItem(headers, 4.0F, 200.0F, null);
    CGItem testItem3 = createItem(headers, 2.0F, 250.0F, null);

    testCluster.insert(testItem);
    testCluster.insert(testItem2);

    HashMap<String, Range<Float>> globalRanges = new HashMap<>();
    globalRanges.put(headers.get(0), Range.between(0.0F, 100.0F));
    globalRanges.put(headers.get(1), Range.between(0.0F, 1000.0F));

    float tupleEnlargementValue = testCluster.tupleEnlargement(testItem3, globalRanges);
    Assert.assertEquals(0.01, tupleEnlargementValue, 0.001F);

    // the case when the enlargement is zero
    CGItem testItem4 = createItem(headers, 2.0F, 200.0F,  null);

    Cluster testCluster2 = new Cluster(headers);
    testCluster2.insert(testItem4);
    testCluster2.insert(testItem);

    float tupleEnlargementValue2 = testCluster2.tupleEnlargement(testItem3, globalRanges);
    Assert.assertEquals(0, tupleEnlargementValue2, 0.01F);
  }

  /**
   * Testing the cluster enlargement
   */
  @Test
  public void clusterEnlargementTest() {
    String[] array = {"timeseries_id", "Seconds_EnergyConsumption"};
    List<String> headers = Arrays.asList(array);
    Cluster testCluster = new Cluster(headers);
    Cluster testCluster2 = new Cluster(headers);
    Cluster testCluster3 = new Cluster(headers);
    Cluster testCluster4 = new Cluster(headers);

    HashMap<String, Range<Float>> globalRanges = new HashMap<>();
    globalRanges.put(headers.get(0), Range.between(0.0F, 100.0F));
    globalRanges.put(headers.get(1), Range.between(0.0F, 1000.0F));

    CGItem testItem = createItem(headers, 5.0F, 300.0F,  null);
    CGItem testItem2 = createItem(headers, 4.0F, 200.0F, null);
    CGItem testItem3 = createItem(headers, 2.0F, 250.0F, null);
    CGItem testItem4 = createItem(headers, 1.0F, 500.0F, null);

    testCluster.insert(testItem);
    testCluster.insert(testItem2);

    testCluster2.insert(testItem3);
    testCluster2.insert(testItem4);

//    System.out.println(testCluster.getRanges()); //-> [4;5] , [200;300]
//    System.out.println(testCluster2.getRanges()); //-> [1;2] , [250;500]
//    Expected ranges after merging: [1;5], [200;500]

    float enlargementValue = testCluster.clusterEnlargement(testCluster2, globalRanges);

    float expectedValue = 0.23F / 2;
    Assert.assertEquals(expectedValue,enlargementValue,0F);

    testCluster3.insert(testItem);
    testCluster3.insert(testItem4);
    testCluster4.insert(testItem2);
    testCluster4.insert(testItem3);

//    System.out.println(testCluster3.getRanges()); //-> [1;5] , [300;500]
//    System.out.println(testCluster4.getRanges()); //-> [2;4] , [200;250]

    float enlargementValue2 = testCluster3.clusterEnlargement(testCluster4, globalRanges);
    Assert.assertEquals(0.05,enlargementValue2,0.00001F);

  }

  /**
   * Testing the information loss with a tuple
   */
  @Test
  public void informationLossGivenTTest() {
    String[] array = {"timeseries_id", "Seconds_EnergyConsumption"};
    List<String> headers = Arrays.asList(array);
    Cluster testCluster = new Cluster(headers);

    HashMap<String, Range<Float>> globalRanges = new HashMap<>();
    globalRanges.put(headers.get(0), Range.between(0.0F, 100.0F));
    globalRanges.put(headers.get(1), Range.between(0.0F, 1000.0F));

    CGItem one = createItem(headers, 1.0F, 200.0F, null);
    CGItem two = createItem(headers, 5.0F, 300.0F, null);
    CGItem three = createItem(headers, 2.0F, 250.0F, null);

    float loss1 = testCluster.informationLossGivenT(one, globalRanges);
    Assert.assertEquals(0.0F, loss1, 0.0F);

    testCluster.insert(one);

    float loss2 = testCluster.informationLossGivenT(two, globalRanges);
    Assert.assertEquals(0.14F, loss2, 0.0F);

    testCluster.insert(two);

    float loss3 = testCluster.informationLossGivenT(three, globalRanges);
    Assert.assertEquals(0.14F, loss3, 0.0F);
  }

  /**
   * Testing the information loss with a cluster
   */
  @Test
  public void informationLossGivenCTest() {
    String[] array = {"timeseries_id", "Seconds_EnergyConsumption"};
    List<String> headers = Arrays.asList(array);
    Cluster testClusterOne = new Cluster(headers);
    Cluster testClusterTwo = new Cluster(headers);

    HashMap<String, Range<Float>> globalRanges = new HashMap<>();
    globalRanges.put(headers.get(0), Range.between(0.0F, 100.0F));
    globalRanges.put(headers.get(1), Range.between(0.0F, 1000.0F));

    CGItem one = createItem(headers,1.0F, 200.0F,  null);
    CGItem two = createItem(headers, 5.0F, 300.0F, null);
    CGItem three = createItem(headers, 2.0F, 250.0F, null);
    CGItem four = createItem(headers, 2.0F, 100.0F, null);

    float loss0 = testClusterOne.informationLossGivenC(testClusterTwo, globalRanges);
    Assert.assertEquals(0.0F, loss0, 0.0F);

    testClusterOne.insert(one);
    float loss1 = testClusterOne.informationLossGivenC(testClusterTwo, globalRanges);
    Assert.assertEquals(0.0F, loss1, 0.0F);

    testClusterOne.insert(two);
    float loss2 = testClusterOne.informationLossGivenC(testClusterTwo, globalRanges);
    Assert.assertEquals(0.14F, loss2, 0.0F);

    testClusterTwo.insert(three);
    float loss3 = testClusterOne.informationLossGivenC(testClusterTwo, globalRanges);
    Assert.assertEquals(0.14F, loss3, 0.0F);

    testClusterTwo.insert(four);
    float loss4 = testClusterOne.informationLossGivenC(testClusterTwo, globalRanges);
    Assert.assertEquals(0.24F, loss4, 0.000001F);
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
    Cluster testCluster = new Cluster(headers);

    HashMap<String, Range<Float>> globalRanges = new HashMap<>();
    globalRanges.put(headers.get(0), Range.between(0.0F, 100.0F));
    globalRanges.put(headers.get(1), Range.between(0.0F, 1000.0F));

    CGItem one = createItem(headers, 1.0F, 200.0F, null);
    CGItem two = createItem(headers, 5.0F, 300.0F, null);
    CGItem three = createItem(headers, 2.0F, 250.0F, null);

    float loss0 = testCluster.informationLoss(globalRanges);
    Assert.assertEquals(0.0F, loss0, 0.0F);

    testCluster.insert(one);
    float loss1 = testCluster.informationLoss(globalRanges);
    Assert.assertEquals(0.0F, loss1, 0.0F);

    testCluster.insert(two);
    float loss2 = testCluster.informationLoss(globalRanges);
    Assert.assertEquals(0.14F, loss2, 0.0F);

    testCluster.insert(three);
    float loss3 = testCluster.informationLoss(globalRanges);
    Assert.assertEquals(0.14F, loss3, 0.0F);
  }

  /**
   * Testing the distance
   */
  @Test
  public void distanceTest() {
    String[] array = {"timeseries_id", "Seconds_EnergyConsumption"};
    List<String> headers = Arrays.asList(array);

    CGItem one = createItem(headers, 1.0F, 200.0F, null);
    CGItem two = createItem(headers, 4.0F, 400.0F, null);

    Cluster cluster = new Cluster(headers);

    cluster.insert(one);
    cluster.insert(two);

    float dist = cluster.distance(two);
    Assert.assertEquals(dist, 201.0F, 0.0F);
    }

  /**
   * Testing the without bounds method
   */
  @Test
  public void withinBoundsTest() {
    String[] array = {"timeseries_id", "Seconds_EnergyConsumption"};
    List<String> headers = Arrays.asList(array);

    CGItem one = createItem(headers, 1.0F, 200.0F, null);
    CGItem two = createItem(headers, 4.0F, 400.0F, null);

    Cluster cluster = new Cluster(headers);

    Assert.assertFalse(cluster.withinBounds(one));
    cluster.insert(one);
    Assert.assertTrue(cluster.withinBounds(one));
    Assert.assertFalse(cluster.withinBounds(two));
    cluster.insert(two);
    Assert.assertTrue(cluster.withinBounds(two));
  }
}