package com.ganges.lib.castleguard;

import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class ClusterTest {

    @Test
    public void insert() {
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

        Item testItem = new Item(data, headers,"Seconds_EnergyConsumption");
        Item testItem2 = new Item(data, headers,"Seconds_EnergyConsumption");
        Item testItem3 = new Item(data, headers,"Seconds_EnergyConsumption");

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

    Item element(float spc_timeseries, float spc_EngergyConsumption, List<String> headers){
        HashMap<String, Float> expected_data = new HashMap<>();
        expected_data.put("mintimeseries_id", 1.0F);
        expected_data.put("spctimeseries_id", spc_timeseries);
        expected_data.put("maxtimeseries_id", 4.0F);
        expected_data.put("minSeconds_EnergyConsumption", 200.0F);
        expected_data.put("spcSeconds_EnergyConsumption",spc_EngergyConsumption);
        expected_data.put("maxSeconds_EnergyConsumption", 400.0F);
        return new Item(expected_data,headers,null);
    }

    @Test
    public void generalise() {
        ArrayList<String> headers = new ArrayList<>();
        headers.add("timeseries_id");
        headers.add("Seconds_EnergyConsumption");

        HashMap<String, Float> data_one = new HashMap<>();
        data_one.put(headers.get(0), 1.0F);
        data_one.put(headers.get(1), 200.0F);

        Item one = new Item(data_one,headers,null);

        HashMap<String, Float> data_two = new HashMap<>();
        data_two.put(headers.get(0), 4.0F);
        data_two.put(headers.get(1), 400.0F);

        Item two = new Item(data_two,headers,null);
        Cluster cluster = new Cluster(headers);

        cluster.insert(one);
        cluster.insert(two);
        Item result = cluster.generalise(one);

        String[] gen_array = {"mintimeseries_id", "spctimeseries_id", "maxtimeseries_id", "minSeconds_EnergyConsumption", "spcSeconds_EnergyConsumption", "maxSeconds_EnergyConsumption"};
        List<String> gen_headers = Arrays.asList(gen_array);

        Item expected_item_one = element(1.0F, 200.0F, gen_headers);
        Item expected_item_two = element(4.0F, 200.0F, gen_headers);
        Item expected_item_three = element(1.0F, 400.0F, gen_headers);
        Item expected_item_four = element(4.0F, 200.0F, gen_headers);
        Assert.assertTrue(result.getData().equals(expected_item_one.getData()) || result.getData().equals(expected_item_two.getData()) || result.getData().equals(expected_item_three.getData()) || result.getData().equals(expected_item_four.getData()));
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
    public void distance() {
        ArrayList<String> headers = new ArrayList<>();
        headers.add("timeseries_id");
        headers.add("Seconds_EnergyConsumption");

        HashMap<String, Float> data_one = new HashMap<>();
        data_one.put(headers.get(0), 1.0F);
        data_one.put(headers.get(1), 200.0F);

        Item one = new Item(data_one,headers,null);

        HashMap<String, Float> data_two = new HashMap<>();
        data_two.put(headers.get(0), 4.0F);
        data_two.put(headers.get(1), 400.0F);

        Item two = new Item(data_two,headers,null);
        Cluster cluster = new Cluster(headers);

        cluster.insert(one);

        float dist = cluster.distance(two);
        System.out.println(dist);
        Assert.assertEquals(dist, 203.0F);


    }

    @Test
    public void within_bounds() {
        ArrayList<String> headers = new ArrayList<>();
        headers.add("timeseries_id");
        headers.add("Seconds_EnergyConsumption");

        HashMap<String, Float> data_one = new HashMap<>();
        data_one.put(headers.get(0), 1.0F);
        data_one.put(headers.get(1), 200.0F);

        Item one = new Item(data_one,headers,null);

        HashMap<String, Float> data_two = new HashMap<>();
        data_two.put(headers.get(0), 4.0F);
        data_two.put(headers.get(1), 400.0F);

        Item two = new Item(data_two,headers,null);
        Cluster cluster = new Cluster(headers);
        Assert.assertFalse(cluster.within_bounds(one));
        cluster.insert(one);
        Assert.assertTrue(cluster.within_bounds(one));
        Assert.assertFalse(cluster.within_bounds(two));
        cluster.insert(two);
        Assert.assertTrue(cluster.within_bounds(two));
    }
}