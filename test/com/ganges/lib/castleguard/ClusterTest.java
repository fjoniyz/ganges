package com.ganges.lib.castleguard;

import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class ClusterTest {
    // Cluster test = new Cluster();

    @Test
    public void insert() {
        String[] array = {"timeseries_id","Seconds_EnergyConsumption"};
        List<String> headers = Arrays.asList(array);
        Cluster testCluster = new Cluster(headers);
        HashMap<String, Float> data = new HashMap<>();
        data.put("pid", 3.0F);
        data.put("timeseries_id", 1.0F);
        data.put("Seconds_EnergyConsumption", 2.0F);

        Item testItem = new Item(data, headers,4.0F);

        testCluster.insert(testItem);

        List<Item> contents = testCluster.getContents();
        Set<Float> diversity = testCluster.getDiversity();

        List<Item> expected_contents = new ArrayList<Item>();
        expected_contents.add(testItem);

        Set<Float> expected_diversity = new HashSet<Float>();
        expected_diversity.add(4.0F);

        Assert.assertEquals(expected_contents,contents);
        Assert.assertEquals(expected_diversity, diversity);
    }

    @Test
    public void remove() {
        String[] array = {"timeseries_id","Seconds_EnergyConsumption"};
        List<String> headers = Arrays.asList(array);
        Cluster testCluster = new Cluster(headers);
        HashMap<String, Float> data = new HashMap<>();
        data.put("pid", 3.0F);
        data.put("timeseries_id", 1.0F);
        data.put("Seconds_EnergyConsumption", 2.0F);

        Item testItem = new Item(data, headers,4.0F);
        Item testItem2 = new Item(data, headers,5.0F);
        Item testItem3 = new Item(data, headers,4.0F);

        testCluster.insert(testItem);
        testCluster.insert(testItem2);
        testCluster.insert(testItem3);
        List<Item> contents = testCluster.getContents();

        List<Item> expected_contents = new ArrayList<Item>();
        expected_contents.add(testItem);
        expected_contents.add(testItem2);
        expected_contents.add(testItem3);
        Set<Float> expected_diversity = new HashSet<Float>();
        expected_diversity.add(4.0F);
        expected_diversity.add(5.0F);

        Assert.assertEquals(expected_contents,contents);

        testCluster.remove(testItem);
        contents = testCluster.getContents();
        Set<Float> diversity = testCluster.getDiversity();
        expected_contents.remove(testItem);

        Assert.assertEquals(expected_contents,contents);
        Assert.assertEquals(expected_diversity, diversity);
    }

    @Test
    public void generalise() {
    }

    @Test
    public void tuple_enlargement() {
    }

    @Test
    public void cluster_enlargement() {
    }

    @Test
    public void information_loss_given_t() {
    }

    @Test
    public void information_loss_given_c() {
    }

    @Test
    public void information_loss() {
        //Create a cluster with 3 items inside
        String[] array = {"timeseries_id","Seconds_EnergyConsumption"};
        List<String> headers = Arrays.asList(array);
        Cluster testCluster = new Cluster(headers);
        HashMap<String, Float> data = new HashMap<>();
        data.put("pid", 3.0F);
        data.put("timeseries_id", 1.0F);
        data.put("Seconds_EnergyConsumption", 2.0F);

        Item testItem = new Item(data, headers,4.0F);
        Item testItem2 = new Item(data, headers,5.0F);
        Item testItem3 = new Item(data, headers,4.0F);

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
    public void distance() {
    }

    @Test
    public void within_bounds() {
    }
}