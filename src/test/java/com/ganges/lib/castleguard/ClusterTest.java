package com.ganges.lib.castleguard;

import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class ClusterTest {

    @Test
    public void insertTest() {
        String[] array = {"timeseries_id","Seconds_EnergyConsumption"};
        List<String> headers = Arrays.asList(array);
        Cluster testCluster = new Cluster(headers);
        HashMap<String, Float> data = new HashMap<>();
        data.put("pid", 3.0F);
        data.put("timeseries_id", 1.0F);
        data.put("Seconds_EnergyConsumption", 2.0F);

        Item testItem = new Item(data, headers,"Seconds_EnergyConsumption");

        testCluster.insert(testItem);

        List<Item> contents = testCluster.getContents();
        Set<Float> diversity = testCluster.getDiversity();

        List<Item> expectedContents = new ArrayList<Item>();
        expectedContents.add(testItem);

        Set<Float> expectedDiversity = new HashSet<Float>();
        expectedDiversity.add(4.0F);

        Assert.assertEquals(expectedContents,contents);
        Assert.assertEquals(expectedDiversity, diversity);
    }

    @Test
    public void removeTest() {
        String[] array = {"timeseries_id","Seconds_EnergyConsumption"};
        List<String> headers = Arrays.asList(array);

        Cluster testCluster = new Cluster(headers);
        HashMap<String, Float> data = new HashMap<>();
        data.put("pid", 3.0F);
        data.put("timeseries_id", 1.0F);
        data.put("Seconds_EnergyConsumption", 2.0F);

        Item testItem = new Item(data, headers,"Seconds_EnergyConsumption");
        Item testItem2 = new Item(data, headers,"Seconds_EnergyConsumption");
        Item testItem3 = new Item(data, headers,"Seconds_EnergyConsumption");

        testCluster.insert(testItem);
        testCluster.insert(testItem2);
        testCluster.insert(testItem3);
        List<Item> contents = testCluster.getContents();

        List<Item> expectedContents = new ArrayList<Item>();
        expectedContents.add(testItem);
        expectedContents.add(testItem2);
        expectedContents.add(testItem3);
        Set<Float> expectedDiversity = new HashSet<Float>();
        expectedDiversity.add(4.0F);
        expectedDiversity.add(5.0F);

        Assert.assertEquals(expectedContents,contents);

        testCluster.remove(testItem);
        contents = testCluster.getContents();
        Set<Float> diversity = testCluster.getDiversity();
        expectedContents.remove(testItem);

        Assert.assertEquals(expectedContents,contents);
        Assert.assertEquals(expectedDiversity, diversity);
    }

    Item element(float spcTimeseries, float spcEngergyConsumption, List<String> headers){
        HashMap<String, Float> expectedData = new HashMap<>();
        expectedData.put("mintimeseries_id", 1.0F);
        expectedData.put("spctimeseries_id", spcTimeseries);
        expectedData.put("maxtimeseries_id", 4.0F);
        expectedData.put("minSeconds_EnergyConsumption", 200.0F);
        expectedData.put("spcSeconds_EnergyConsumption",spcEngergyConsumption);
        expectedData.put("maxSeconds_EnergyConsumption", 400.0F);
        return new Item(expectedData,headers,null);
    }

    @Test
    public void generaliseTest() {
        ArrayList<String> headers = new ArrayList<>();
        headers.add("timeseries_id");
        headers.add("Seconds_EnergyConsumption");

        HashMap<String, Float> dataOne = new HashMap<>();
        dataOne.put(headers.get(0), 1.0F);
        dataOne.put(headers.get(1), 200.0F);

        Item one = new Item(dataOne,headers,null);

        HashMap<String, Float> dataTwo = new HashMap<>();
        dataTwo.put(headers.get(0), 4.0F);
        dataTwo.put(headers.get(1), 400.0F);

        Item two = new Item(dataTwo,headers,null);
        Cluster cluster = new Cluster(headers);

        cluster.insert(one);
        cluster.insert(two);
        Item result = cluster.generalise(one);

        String[] genArray = {"mintimeseries_id", "spctimeseries_id", "maxtimeseries_id", "minSeconds_EnergyConsumption", "spcSeconds_EnergyConsumption", "maxSeconds_EnergyConsumption"};
        List<String> genHeaders = Arrays.asList(genArray);

        Item expectedItemOne = element(1.0F, 200.0F, genHeaders);
        Item expectedItemTwo = element(4.0F, 200.0F, genHeaders);
        Item expectedItemThree = element(1.0F, 400.0F, genHeaders);
        Item expectedItemFour = element(4.0F, 200.0F, genHeaders);
        Assert.assertTrue(result.getData().equals(expectedItemOne.getData()) || result.getData().equals(expectedItemTwo.getData()) || result.getData().equals(expectedItemThree.getData()) || result.getData().equals(expectedItemFour.getData()));
    }

    @Test
    public void tupleEnlargementTest() {
    }

    @Test
    public void clusterEnlargementTest() {
    }

    @Test
    public void informationLossGivenTTest() {
    }

    @Test
    public void informationLossGivenCTest() {
    }

    @Test
    public void informationLossTest() {
        //Create a cluster with 3 items inside
        String[] array = {"timeseries_id","Seconds_EnergyConsumption"};
        List<String> headers = Arrays.asList(array);
        Cluster testCluster = new Cluster(headers);
        HashMap<String, Float> data = new HashMap<>();
        data.put("pid", 3.0F);
        data.put("timeseries_id", 1.0F);
        data.put("Seconds_EnergyConsumption", 2.0F);

        Item testItem = new Item(data, headers,"Seconds_EnergyConsumption");
        Item testItem2 = new Item(data, headers,"Seconds_EnergyConsumption");
        Item testItem3 = new Item(data, headers,"Seconds_EnergyConsumption");

        testCluster.insert(testItem);
        testCluster.insert(testItem2);
        testCluster.insert(testItem3);

//        System.out.println(testCluster.getRanges());
//
//        Float loss = testCluster.information_loss(testCluster.getRanges());
//
//        System.out.println(loss);
    }

    @Test
    public void distanceTest() {
        ArrayList<String> headers = new ArrayList<>();
        headers.add("timeseries_id");
        headers.add("Seconds_EnergyConsumption");

        HashMap<String, Float> dataOne = new HashMap<>();
        dataOne.put(headers.get(0), 1.0F);
        dataOne.put(headers.get(1), 200.0F);

        Item one = new Item(dataOne,headers,null);

        HashMap<String, Float> dataTwo = new HashMap<>();
        dataTwo.put(headers.get(0), 4.0F);
        dataTwo.put(headers.get(1), 400.0F);

        Item two = new Item(dataTwo,headers,null);
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

        Item one = new Item(dataOne,headers,null);

        HashMap<String, Float> dataTwo = new HashMap<>();
        dataTwo.put(headers.get(0), 4.0F);
        dataTwo.put(headers.get(1), 400.0F);

        Item two = new Item(dataTwo,headers,null);
        Cluster cluster = new Cluster(headers);
        Assert.assertFalse(cluster.withinBounds(one));
        cluster.insert(one);
        Assert.assertTrue(cluster.withinBounds(one));
        Assert.assertFalse(cluster.withinBounds(two));
        cluster.insert(two);
        Assert.assertTrue(cluster.withinBounds(two));
    }
}