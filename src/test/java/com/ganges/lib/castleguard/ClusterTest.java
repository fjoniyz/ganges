package com.ganges.lib.castleguard;

import org.junit.Assert;
import org.junit.Test;
import org.apache.commons.lang3.Range;

import java.util.*;
import org.apache.commons.lang3.Range;

public class ClusterTest {

  @Test
  public void insertTest() {
    String[] array = {"timeseries_id", "Seconds_EnergyConsumption"};
    List<String> headers = Arrays.asList(array);
    Cluster testCluster = new Cluster(headers);
    HashMap<String, Float> data = new HashMap<>();
    data.put("pid", 3.0F);
    data.put("timeseries_id", 1.0F);
    data.put("Seconds_EnergyConsumption", 2.0F);
    Map<String, Range<Float>> expectedRanges = new LinkedHashMap<>();
    headers.forEach(header -> expectedRanges.put(header, Range.between(0F, 0F)));

    Item testItem = new Item(data, headers, "Seconds_EnergyConsumption");

    testCluster.insert(testItem);

    List<Item> contents = testCluster.getContents();
    Set<Float> diversity = testCluster.getDiversity();

    List<Item> expectedContents = new ArrayList<Item>();
    expectedContents.add(testItem);

    Set<Float> expectedDiversity = new HashSet<Float>();
    expectedDiversity.add(2.0F);

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

    HashMap<String, Float> data2 = new HashMap<>();
    data2.put("pid", 4.0F);
    data2.put("timeseries_id", 3.0F);
    data2.put("Seconds_EnergyConsumption", 4.0F);

    Item testItem2 = new Item(data2, headers, "Seconds_EnergyConsumption");

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

  @Test
  public void removeTest() {
    String[] array = {"timeseries_id", "Seconds_EnergyConsumption"};
    List<String> headers = Arrays.asList(array);

    Cluster testCluster = new Cluster(headers);
    HashMap<String, Float> data = new HashMap<>();
    data.put("timeseries_id", 1.0F);
    data.put("Seconds_EnergyConsumption", 2.0F);

    HashMap<String, Float> data2 = new HashMap<>();
    data2.put("timeseries_id", 5.0F);
    data2.put("Seconds_EnergyConsumption", 1.0F);

    HashMap<String, Float> data3 = new HashMap<>();
    data3.put("timeseries_id", 4.0F);
    data3.put("Seconds_EnergyConsumption", 6.0F);

    Item testItem = new Item(data, headers, "Seconds_EnergyConsumption");
    Item testItem2 = new Item(data2, headers, "Seconds_EnergyConsumption");
    Item testItem3 = new Item(data3, headers, "Seconds_EnergyConsumption");

    testCluster.insert(testItem);
    testCluster.insert(testItem2);
    testCluster.insert(testItem3);
    List<Item> contents = testCluster.getContents();

    List<Item> expectedContents = new ArrayList<Item>();
    expectedContents.add(testItem);
    expectedContents.add(testItem2);
    expectedContents.add(testItem3);
    Set<Float> expectedDiversity = new HashSet<Float>();
    expectedDiversity.add(1.0F);
    expectedDiversity.add(6.0F);

    Assert.assertEquals(expectedContents, contents);

    testCluster.remove(testItem);
    contents = testCluster.getContents();
    Set<Float> diversity = testCluster.getDiversity();
    expectedContents.remove(testItem);


    Assert.assertEquals(expectedContents, contents);
    Assert.assertEquals(expectedDiversity, diversity);

    Map<String, Range<Float>> expectedRanges = new LinkedHashMap<>();
    expectedRanges.put("timeseries_id", Range.between(4F, 5F));
    expectedRanges.put("Seconds_EnergyConsumption", Range.between(1F, 6F));

    Assert.assertEquals(expectedRanges, testCluster.getRanges());

    testCluster.insert(testItem);
    testCluster.remove(testItem2);

    expectedRanges.replace("timeseries_id", Range.between(4F, 5F),Range.between(1F, 4F));
    expectedRanges.replace("Seconds_EnergyConsumption", Range.between(1F, 6F), Range.between(2F, 6F));

    Assert.assertEquals(expectedRanges, testCluster.getRanges());

    Cluster testCluster2 = new Cluster(headers);

    testCluster2.insert(testItem);
    testCluster2.insert(testItem2);
    testCluster2.insert(testItem3);

    testCluster2.remove(testItem2);
    Assert.assertEquals(expectedRanges, testCluster2.getRanges());
  }

  Item element(float spcTimeseries, float spcEngergyConsumption, List<String> headers) {
    HashMap<String, Float> expectedData = new HashMap<>();
    expectedData.put("mintimeseries_id", 1.0F);
    expectedData.put("spctimeseries_id", spcTimeseries);
    expectedData.put("maxtimeseries_id", 4.0F);
    expectedData.put("minSeconds_EnergyConsumption", 200.0F);
    expectedData.put("spcSeconds_EnergyConsumption", spcEngergyConsumption);
    expectedData.put("maxSeconds_EnergyConsumption", 400.0F);
    return new Item(expectedData, headers, null);
  }

  @Test
  public void generaliseTest() {
    ArrayList<String> headers = new ArrayList<>();
    headers.add("timeseries_id");
    headers.add("Seconds_EnergyConsumption");

    HashMap<String, Float> dataOne = new HashMap<>();
    dataOne.put(headers.get(0), 1.0F);
    dataOne.put(headers.get(1), 200.0F);

    Item one = new Item(dataOne, headers, null);

    HashMap<String, Float> dataTwo = new HashMap<>();
    dataTwo.put(headers.get(0), 4.0F);
    dataTwo.put(headers.get(1), 400.0F);

    Item two = new Item(dataTwo, headers, null);
    Cluster cluster = new Cluster(headers);

    cluster.insert(one);
    cluster.insert(two);
    Item result = cluster.generalise(one);

    String[] genArray = {
      "mintimeseries_id",
      "spctimeseries_id",
      "maxtimeseries_id",
      "minSeconds_EnergyConsumption",
      "spcSeconds_EnergyConsumption",
      "maxSeconds_EnergyConsumption"
    };
    List<String> genHeaders = Arrays.asList(genArray);

    Item expectedItemOne = element(1.0F, 200.0F, genHeaders);
    Item expectedItemTwo = element(4.0F, 200.0F, genHeaders);
    Item expectedItemThree = element(1.0F, 400.0F, genHeaders);
    Item expectedItemFour = element(4.0F, 200.0F, genHeaders);
    Assert.assertTrue(
        result.getData().equals(expectedItemOne.getData())
            || result.getData().equals(expectedItemTwo.getData())
            || result.getData().equals(expectedItemThree.getData())
            || result.getData().equals(expectedItemFour.getData()));
  }

  @Test
  public void tupleEnlargementTest() {
    String[] array = {"timeseries_id", "Seconds_EnergyConsumption"};
    List<String> headers = Arrays.asList(array);
    Cluster testCluster = new Cluster(headers);
    HashMap<String, Float> data = new HashMap<>();
    data.put("timeseries_id", 5.0F);
    data.put("Seconds_EnergyConsumption", 300.0F);

    Item testItem = new Item(data, headers, null);

    HashMap<String, Float> data2 = new HashMap<>();
    data2.put("timeseries_id", 4.0F);
    data2.put("Seconds_EnergyConsumption", 200.0F);

    Item testItem2 = new Item(data2, headers, null);

    HashMap<String, Float> data3 = new HashMap<>();
    data3.put("timeseries_id", 2.0F);
    data3.put("Seconds_EnergyConsumption", 250.0F);

    Item testItem3 = new Item(data3, headers, null);

    testCluster.insert(testItem);
    testCluster.insert(testItem2);

    HashMap<String, Range<Float>> globalRanges = new HashMap<>();
    globalRanges.put(headers.get(0), Range.between(0.0F, 100.0F));
    globalRanges.put(headers.get(1), Range.between(0.0F, 1000.0F));

    float tupleEnlargementValue = testCluster.tupleEnlargement(testItem3, globalRanges);
    Assert.assertEquals(0.01, tupleEnlargementValue, 0.001F);

    // the case when the enlargement is zero

    HashMap<String, Float> data4 = new HashMap<>();
    data4.put("timeseries_id", 2.0F);
    data4.put("Seconds_EnergyConsumption", 200.0F);

    Item testItem4 = new Item(data4, headers, null);

    Cluster testCluster2 = new Cluster(headers);
    testCluster2.insert(testItem4);
    testCluster2.insert(testItem);

    float tupleEnlargementValue2 = testCluster2.tupleEnlargement(testItem3, globalRanges);
    Assert.assertEquals(0, tupleEnlargementValue2, 0.01F);
  }

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

    HashMap<String, Float> data = new HashMap<>();
    data.put("timeseries_id", 5.0F);
    data.put("Seconds_EnergyConsumption", 300.0F);
    Item testItem = new Item(data, headers, null);

    HashMap<String, Float> data2 = new HashMap<>();
    data2.put("timeseries_id", 4.0F);
    data2.put("Seconds_EnergyConsumption", 200.0F);
    Item testItem2 = new Item(data2, headers, null);

    HashMap<String, Float> data3 = new HashMap<>();
    data3.put("timeseries_id", 2.0F);
    data3.put("Seconds_EnergyConsumption", 250.0F);
    Item testItem3 = new Item(data3, headers, null);

    HashMap<String, Float> data4 = new HashMap<>();
    data4.put("timeseries_id", 1.0F);
    data4.put("Seconds_EnergyConsumption", 500.0F);
    Item testItem4 = new Item(data4, headers, null);

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

  @Test
  public void informationLossGivenTTest() {
    ArrayList<String> headers = new ArrayList<>();
    headers.add("timeseries_id");
    headers.add("SecondsEnergyConsumption");
    Cluster testCluster = new Cluster(headers);

    HashMap<String, Range<Float>> globalRanges = new HashMap<>();
    globalRanges.put(headers.get(0), Range.between(0.0F, 100.0F));
    globalRanges.put(headers.get(1), Range.between(0.0F, 1000.0F));

    HashMap<String, Float> dataOne = new HashMap<>();
    dataOne.put(headers.get(0), 1.0F);
    dataOne.put(headers.get(1), 200.0F);

    Item one = new Item(dataOne, headers, null);

    HashMap<String, Float> dataTwo = new HashMap<>();
    dataTwo.put(headers.get(0), 5.0F);
    dataTwo.put(headers.get(1), 300.0F);

    Item two = new Item(dataTwo, headers, null);

    HashMap<String, Float> dataThree = new HashMap<>();
    dataThree.put(headers.get(0), 2.0F);
    dataThree.put(headers.get(1), 250.0F);

    Item three = new Item(dataThree, headers, null);

    float loss1 = testCluster.informationLossGivenT(one, globalRanges);
    Assert.assertEquals(0.0F, loss1, 0.0F);

    testCluster.insert(one);

    float loss2 = testCluster.informationLossGivenT(two, globalRanges);
    Assert.assertEquals(0.14F, loss2, 0.0F);

    testCluster.insert(two);

    float loss3 = testCluster.informationLossGivenT(three, globalRanges);
    Assert.assertEquals(0.14F, loss3, 0.0F);
  }

  @Test
  public void informationLossGivenCTest() {
    ArrayList<String> headers = new ArrayList<>();
    headers.add("timeseries_id");
    headers.add("SecondsEnergyConsumption");
    Cluster testClusterOne = new Cluster(headers);
    Cluster testClusterTwo = new Cluster(headers);

    HashMap<String, Range<Float>> globalRanges = new HashMap<>();
    globalRanges.put(headers.get(0), Range.between(0.0F, 100.0F));
    globalRanges.put(headers.get(1), Range.between(0.0F, 1000.0F));

    HashMap<String, Float> dataOne = new HashMap<>();
    dataOne.put(headers.get(0), 1.0F);
    dataOne.put(headers.get(1), 200.0F);

    Item one = new Item(dataOne, headers, null);

    HashMap<String, Float> dataTwo = new HashMap<>();
    dataTwo.put(headers.get(0), 5.0F);
    dataTwo.put(headers.get(1), 300.0F);

    Item two = new Item(dataTwo, headers, null);

    HashMap<String, Float> dataThree = new HashMap<>();
    dataThree.put(headers.get(0), 2.0F);
    dataThree.put(headers.get(1), 250.0F);

    Item three = new Item(dataThree, headers, null);

    HashMap<String, Float> dataFour = new HashMap<>();
    dataFour.put(headers.get(0), 2.0F);
    dataFour.put(headers.get(1), 100.0F);

    Item four = new Item(dataFour, headers, null);

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
    Assert.assertEquals(0.24F, loss4, 0.0F);
  }

  @Test
  public void informationLossTest() {
    // EV_Usage from 100 to 1000 (according to the data generator)
    // number of overall 100 stations
    // Create a cluster with 3 items inside

    ArrayList<String> headers = new ArrayList<>();
    headers.add("timeseries_id");
    headers.add("SecondsEnergyConsumption");
    Cluster testCluster = new Cluster(headers);

    HashMap<String, Range<Float>> globalRanges = new HashMap<>();
    globalRanges.put(headers.get(0), Range.between(0.0F, 100.0F));
    globalRanges.put(headers.get(1), Range.between(0.0F, 1000.0F));

    HashMap<String, Float> dataOne = new HashMap<>();
    dataOne.put(headers.get(0), 1.0F);
    dataOne.put(headers.get(1), 200.0F);

    Item one = new Item(dataOne, headers, null);

    HashMap<String, Float> dataTwo = new HashMap<>();
    dataTwo.put(headers.get(0), 5.0F);
    dataTwo.put(headers.get(1), 300.0F);

    Item two = new Item(dataTwo, headers, null);

    HashMap<String, Float> dataThree = new HashMap<>();
    dataThree.put(headers.get(0), 2.0F);
    dataThree.put(headers.get(1), 250.0F);

    Item three = new Item(dataThree, headers, null);

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

  @Test
  public void distanceTest() {
    ArrayList<String> headers = new ArrayList<>();
    headers.add("timeseries_id");
    headers.add("Seconds_EnergyConsumption");

    HashMap<String, Float> dataOne = new HashMap<>();
    dataOne.put(headers.get(0), 1.0F);
    dataOne.put(headers.get(1), 200.0F);

    Item one = new Item(dataOne, headers, null);

    HashMap<String, Float> dataTwo = new HashMap<>();
    dataTwo.put(headers.get(0), 4.0F);
    dataTwo.put(headers.get(1), 400.0F);

    Item two = new Item(dataTwo, headers, null);
    Cluster cluster = new Cluster(headers);

    cluster.insert(one);

    float dist = cluster.distance(two);
    System.out.println(dist);
    Assert.assertEquals(dist, 203.0F, 0.0F);
  }

  @Test
  public void withinBoundsTest() {
    ArrayList<String> headers = new ArrayList<>();
    headers.add("timeseries_id");
    headers.add("Seconds_EnergyConsumption");

    HashMap<String, Float> dataOne = new HashMap<>();
    dataOne.put(headers.get(0), 1.0F);
    dataOne.put(headers.get(1), 200.0F);

    Item one = new Item(dataOne, headers, null);

    HashMap<String, Float> dataTwo = new HashMap<>();
    dataTwo.put(headers.get(0), 4.0F);
    dataTwo.put(headers.get(1), 400.0F);

    Item two = new Item(dataTwo, headers, null);
    Cluster cluster = new Cluster(headers);
    Assert.assertFalse(cluster.withinBounds(one));
    cluster.insert(one);
    Assert.assertTrue(cluster.withinBounds(one));
    Assert.assertFalse(cluster.withinBounds(two));
    cluster.insert(two);
    Assert.assertTrue(cluster.withinBounds(two));
  }
}
