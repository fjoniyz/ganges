package com.ganges.anonlib.castleguard;

import com.ganges.anonlib.castleguard.utils.ClusterManagement;
import com.ganges.anonlib.AnonymizationItem;
import org.apache.commons.lang3.Range;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class CastleGuardTest {
    private ArrayList<String> headers;
    private HashMap<String, Range<Double>> globalRanges;
    private CGConfig config;
    private CastleGuard castle;

    /**
     * creates an objects of CastleGuard, and CGConfig;
     * @ params: setting for config
     */
    private void preparation(int k, int delta, int beta, int bigBeta, int mu, int l, double phi, boolean useDiffPrivacy){
        this.headers = new ArrayList<>();
        this.headers.add("timeseriesId");
        this.headers.add("SecondsEnergyConsumption");
        this.headers.add("station");

        this.globalRanges = new HashMap<>();
        this.globalRanges.put(headers.get(0), Range.between(1.0, 1.0));
        this.globalRanges.put(headers.get(1), Range.between(200.0, 200.0));
        this.globalRanges.put(headers.get(2), Range.between(5.0, 5.0));

        this.config = new CGConfig(k, delta, beta, bigBeta, mu, l, phi, useDiffPrivacy);
        this.castle = new CastleGuard(this.config, this.headers, "station");
    }

    /**
     * creates an item with double Values;
     * @ param: list of data for given headers
     */
    public HashMap<String, Double> createDoubleItem(List<Double> elements){
        Assert.assertEquals(this.headers.size(), elements.size());
        HashMap<String, Double> item = new HashMap<>();
        int i = 0;
        for (String header: this.headers) {
            item.put(header, elements.get(i));
            i++;
        }
        return item;
    }

    /**
     * creates an item;
     * @ param: list of data for given headers
     */
    public HashMap<String, Double> createItem(List<Double> elements){
        Assert.assertEquals(this.headers.size(), elements.size());
        HashMap<String, Double> item = new HashMap<>();
        int i = 0;
        for (String header: this.headers) {
            item.put(header, elements.get(i));
            i++;
        }
        return item;
    }

    /**
     * Tests the first data insertion of insertData() with Differential Privacy = true
     */
    @Test
    public void insertFirstDataTestWithPrivacy() {
        preparation(3, 10, 5, 1, 5, 1, 100 * Math.log(2), true);

        HashMap<String, Double> dataOne = createItem(Arrays.asList(1.0, 200.0, 5.0));

        ClusterManagement manage = this.castle.getClusterManagement();
        List<Cluster> clusters = manage.getNonAnonymizedClusters();
        Assert.assertTrue(clusters.isEmpty());
        castle.insertData(dataOne);
        clusters = manage.getNonAnonymizedClusters();
        Assert.assertNotEquals(clusters.get(0).getContents().get(0).getData().get("station"), 5.0, 0.0);
        Assert.assertFalse(clusters.isEmpty());
        Assert.assertEquals(globalRanges, this.castle.getGlobalRanges());
      }

    /**
     * Tests the first data insertion of insertData() with Differential Privacy = false
     */
    @Test
    public void insertFirstDataTestWithoutPrivacy() {
        preparation(3, 10, 5, 1, 5, 1, 100 * Math.log(2), false);
        HashMap<String, Double> dataOne = createItem(Arrays.asList(1.0, 200.0, 5.0));

        ClusterManagement manage = this.castle.getClusterManagement();
        castle.insertData(dataOne);
        List<Cluster> clusters = manage.getNonAnonymizedClusters();
        Assert.assertEquals(clusters.get(0).getContents().get(0).getData().get("timeseriesId"), 1.0, 0.0);
        Assert.assertEquals(clusters.get(0).getContents().get(0).getData().get("Seconds_EnergyConsumption"), 200.0, 0.0);
        Assert.assertEquals(clusters.get(0).getContents().get(0).getData().get("station"), 5.0, 0.0);
    }

    /**
     * trying with bigBeta = 0; (1 - bigBeta) is the probability to ignore tuple (only with Differential Privacy)
     */
    @Test
    public void insertIgnoreDataTest() {
        preparation(3, 10, 5, 0, 5, 1, 100 * Math.log(2), true);

        HashMap<String, Double> dataOne = createItem(Arrays.asList(1.0, 200.0, 5.0));

        ClusterManagement manage = this.castle.getClusterManagement();
        List<Cluster> clusters = manage.getNonAnonymizedClusters();
        Assert.assertTrue(clusters.isEmpty());
        castle.insertData(dataOne);
        clusters = manage.getNonAnonymizedClusters();
        Assert.assertTrue(clusters.isEmpty());
    }

    @Test
    public void testAnonymize() {
        Map<String, String> testNonAnon = new HashMap<>();
        preparation(3, 10, 5, 1, 5, 1, 100 * Math.log(2), true);
        Map<String, Double> dataOne = createDoubleItem(Arrays.asList(1.0, 200.0, 5.0));
        Map<String, Double> dataTwo = createDoubleItem(Arrays.asList(2.0, 300.0, 5.0));
        Map<String, Double> dataThree = createDoubleItem(Arrays.asList(3.0, 400.0, 3.0));
        Map<String, Double> dataFour = createDoubleItem(Arrays.asList(4.0, 500.0, 2.0));
        Map<String, Double> dataFive = createDoubleItem(Arrays.asList(5.0, 600.0, 1.0));
        Map<String, Double> dataSix = createDoubleItem(Arrays.asList(6.0, 700.0, 0.0));

        List<AnonymizationItem> inputOne = new ArrayList<>();
        inputOne.add(new AnonymizationItem("1", dataOne, testNonAnon));

        List<AnonymizationItem> inputTwo = new ArrayList<>();
        inputTwo.add(new AnonymizationItem("2", dataTwo, testNonAnon));
        inputTwo.add(new AnonymizationItem("3", dataThree, testNonAnon));
        inputTwo.add(new AnonymizationItem("4", dataFour, testNonAnon));

        List<AnonymizationItem> inputThree = new ArrayList<>();
        inputThree.add(new AnonymizationItem("5", dataFive, testNonAnon));
        inputThree.add(new AnonymizationItem("6", dataSix, testNonAnon));

        castle.anonymize(inputOne);
        castle.anonymize(inputTwo);
        castle.anonymize(inputThree);

        Deque<CGItem> items = this.castle.getItems();
        System.out.println(items);

    }


    /**
     * suppressing all elements within CastleGuard algorithm
     */
    @Test
    public void suppressAllItemsTest() {
        preparation(3, 10, 5, 1, 5, 1, 100 * Math.log(2), true);
        HashMap<String, Double> dataOne = createItem(Arrays.asList(1.0, 200.0, 5.0));
        HashMap<String, Double> dataTwo = createItem(Arrays.asList(2.0, 300.0, 5.0));
        HashMap<String, Double> dataThree = createItem(Arrays.asList(3.0, 400.0, 3.0));
        HashMap<String, Double> dataFour = createItem(Arrays.asList(4.0, 500.0, 2.0));
        HashMap<String, Double> dataFive = createItem(Arrays.asList(5.0, 600.0, 1.0));
        HashMap<String, Double> dataSix = createItem(Arrays.asList(6.0, 700.0, 0.0));

        castle.insertData(dataOne);
        castle.insertData(dataTwo);
        castle.insertData(dataThree);
        castle.insertData(dataFour);
        castle.insertData(dataFive);
        castle.insertData(dataSix);
        Deque<CGItem> items =  this.castle.getItems();
        for(CGItem item: items){
            // before the operation the item is within a cluster
            Assert.assertNotEquals(null, item.getCluster());
            Assert.assertTrue(this.castle.getItems().contains(item));
            Cluster parent = null;
            if (item.getCluster().getSize() ==1){
                parent = item.getCluster();
            }
            this.castle.suppressItem(item);
            // the item is not assigned to a cluster
            Assert.assertEquals(null, item.getCluster());
            Assert.assertFalse(this.castle.getItems().contains(item));

            // a cluster is removed with the last element
            ClusterManagement manage = castle.getClusterManagement();
            List<Cluster> clusters = manage.getNonAnonymizedClusters();
            Assert.assertFalse(clusters.contains(parent));

        }
        Assert.assertTrue(this.castle.getItems().isEmpty());
      }

    /**
     * suppressing an element, that is not included within CastleGuard
     */
    @Test
    public void suppressNonItemTest() {
        preparation(3, 10, 5, 1, 5, 1, 100 * Math.log(2), true);
        HashMap<String, Double> data = createItem(Arrays.asList(1.0, 200.0, 5.0));
        CGItem item = new CGItem("1", data , new HashMap<>(), headers,
            "station");;

        castle.suppressItem(item);

    }

    /**
     * Tests an Update of the global Ranges; with multiple elements
     */
    @Test
    public void updateGlobalRangesTest() {
        ArrayList<String> headers = new ArrayList<>();
        headers.add("timeseriesId");
        headers.add("SecondsEnergyConsumption");
        headers.add("station");

        HashMap<String, Range<Double>> globalRanges = new HashMap<>();
        globalRanges.put(headers.get(0), Range.between(1.0, 1.0));
        globalRanges.put(headers.get(1), Range.between(200.0, 200.0));
        globalRanges.put(headers.get(2), Range.between(5.0, 5.0));

        HashMap<String, Double> dataOne = new HashMap<>();
        dataOne.put(headers.get(0), 1.0);
        dataOne.put(headers.get(1), 200.0);
        dataOne.put(headers.get(2), 5.0);

        CGItem one = new CGItem("1", dataOne , new HashMap<>(), headers,
            "station");

        CGConfig config = new CGConfig(3, 10, 5, 1, 5, 1, 100 * Math.log(2), true);
        CastleGuard castle = new CastleGuard(config, headers, null);

        castle.updateGlobalRanges(one);
        Assert.assertEquals(castle.getGlobalRanges(), globalRanges);

        HashMap<String, Range<Double>> newGlobalRanges = new HashMap<>();
        newGlobalRanges.put(headers.get(0), Range.between(1.0, 2.0));
        newGlobalRanges.put(headers.get(1), Range.between(200.0, 300.0));
        newGlobalRanges.put(headers.get(2), Range.between(4.0, 5.0));

        HashMap<String, Double> dataTwo = new HashMap<>();
        dataTwo.put(headers.get(0), 2.0);
        dataTwo.put(headers.get(1), 300.0);
        dataTwo.put(headers.get(2), 4.0);

        CGItem two = new CGItem("2", dataTwo , new HashMap<>(), headers,
            "station");
        castle.updateGlobalRanges(two);
        Assert.assertEquals(castle.getGlobalRanges(), newGlobalRanges);
      }
}