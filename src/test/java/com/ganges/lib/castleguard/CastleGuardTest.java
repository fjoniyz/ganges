package com.ganges.lib.castleguard;

import com.ganges.lib.castleguard.utils.ClusterManagement;
import org.apache.commons.lang3.Range;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;
public class CastleGuardTest {
    private ArrayList<String> headers;
    private HashMap<String, Range<Float>> globalRanges;
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
        this.globalRanges.put(headers.get(0), Range.between(1.0F, 1.0F));
        this.globalRanges.put(headers.get(1), Range.between(200.0F, 200.0F));
        this.globalRanges.put(headers.get(2), Range.between(5.0F, 5.0F));

        this.config = new CGConfig(k, delta, beta, bigBeta, mu, l, phi, useDiffPrivacy);
        this.castle = new CastleGuard(this.config, this.headers, "station");
    }

    /**
     * creates an item;
     * @ param: list of data for given headers
     */
    public HashMap<String, Float> createItem(List<Float> elements){
        Assert.assertEquals(this.headers.size(), elements.size());
        HashMap<String, Float> item = new HashMap<>();
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

        HashMap<String, Float> dataOne = createItem(Arrays.asList(1.0F, 200.0F, 5.0F));

        ClusterManagement manage = this.castle.getClusterManagement();
        List<Cluster> clusters = manage.getNonAnonymizedClusters();
        Assert.assertTrue(clusters.isEmpty());
        castle.insertData(dataOne);
        clusters = manage.getNonAnonymizedClusters();
        Assert.assertNotEquals(clusters.get(0).getContents().get(0).getData().get("station"), 5.0F, 0.0F);
        Assert.assertFalse(clusters.isEmpty());
        Assert.assertEquals(globalRanges, this.castle.getGlobalRanges());
      }

    /**
     * Tests the first data insertion of insertData() with Differential Privacy = false
     */
    @Test
    public void insertFirstDataTestWithoutPrivacy() {
        preparation(3, 10, 5, 1, 5, 1, 100 * Math.log(2), false);
        HashMap<String, Float> dataOne = createItem(Arrays.asList(1.0F, 200.0F, 5.0F));

        ClusterManagement manage = this.castle.getClusterManagement();
        castle.insertData(dataOne);
        List<Cluster> clusters = manage.getNonAnonymizedClusters();
        Assert.assertEquals(clusters.get(0).getContents().get(0).getData().get("timeseriesId"), 1.0F, 0.0F);
        Assert.assertEquals(clusters.get(0).getContents().get(0).getData().get("SecondsEnergyConsumption"), 200.0F, 0.0F);
        Assert.assertEquals(clusters.get(0).getContents().get(0).getData().get("station"), 5.0F, 0.0F);
    }

    /**
     * trying with bigBeta = 0; (1 - bigBeta) is the probability to ignore tuple (only with Differential Privacy)
     */
    @Test
    public void insertIgnoreDataTest() {
        preparation(3, 10, 5, 0, 5, 1, 100 * Math.log(2), true);

        HashMap<String, Float> dataOne = createItem(Arrays.asList(1.0F, 200.0F, 5.0F));

        ClusterManagement manage = this.castle.getClusterManagement();
        List<Cluster> clusters = manage.getNonAnonymizedClusters();
        Assert.assertTrue(clusters.isEmpty());
        castle.insertData(dataOne);
        clusters = manage.getNonAnonymizedClusters();
        Assert.assertTrue(clusters.isEmpty());
    }

    /**
     * suppressing all elements within CastleGuard algorithm
     */
    @Test
    public void suppressAllItemsTest() {
        preparation(3, 10, 5, 1, 5, 1, 100 * Math.log(2), true);
        HashMap<String, Float> dataOne = createItem(Arrays.asList(1.0F, 200.0F, 5.0F));
        HashMap<String, Float> dataTwo = createItem(Arrays.asList(2.0F, 300.0F, 5.0F));
        HashMap<String, Float> dataThree = createItem(Arrays.asList(3.0F, 400.0F, 3.0F));
        HashMap<String, Float> dataFour = createItem(Arrays.asList(4.0F, 500.0F, 2.0F));
        HashMap<String, Float> dataFive = createItem(Arrays.asList(5.0F, 600.0F, 1.0F));
        HashMap<String, Float> dataSix = createItem(Arrays.asList(6.0F, 700.0F, 0.0F));

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
        HashMap<String, Float> data = createItem(Arrays.asList(1.0F, 200.0F, 5.0F));
        CGItem item = new CGItem(data, this.headers, null);

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

        HashMap<String, Range<Float>> globalRanges = new HashMap<>();
        globalRanges.put(headers.get(0), Range.between(1.0F, 1.0F));
        globalRanges.put(headers.get(1), Range.between(200.0F, 200.0F));
        globalRanges.put(headers.get(2), Range.between(5.0F, 5.0F));

        HashMap<String, Float> dataOne = new HashMap<>();
        dataOne.put(headers.get(0), 1.0F);
        dataOne.put(headers.get(1), 200.0F);
        dataOne.put(headers.get(2), 5.0F);

        CGItem one = new CGItem(dataOne, headers, "station");

        CGConfig config = new CGConfig(3, 10, 5, 1, 5, 1, 100 * Math.log(2), true);
        CastleGuard castle = new CastleGuard(config, headers, null);

        castle.updateGlobalRanges(one);
        Assert.assertEquals(castle.getGlobalRanges(), globalRanges);

        HashMap<String, Range<Float>> newGlobalRanges = new HashMap<>();
        newGlobalRanges.put(headers.get(0), Range.between(1.0F, 2.0F));
        newGlobalRanges.put(headers.get(1), Range.between(200.0F, 300.0F));
        newGlobalRanges.put(headers.get(2), Range.between(4.0F, 5.0F));

        HashMap<String, Float> dataTwo = new HashMap<>();
        dataTwo.put(headers.get(0), 2.0F);
        dataTwo.put(headers.get(1), 300.0F);
        dataTwo.put(headers.get(2), 4.0F);

        CGItem two = new CGItem(dataTwo, headers, "station");
        castle.updateGlobalRanges(two);
        Assert.assertEquals(castle.getGlobalRanges(), newGlobalRanges);
      }
}