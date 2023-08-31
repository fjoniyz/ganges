package com.ganges.anonlib.castleguard;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Collections;

import org.apache.commons.lang3.Range;

import com.ganges.anonlib.castleguard.utils.ClusterManagement;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;


public class ClusterManagementTest {

    private final HashMap<String, Range<Double>> globalRanges = new HashMap<>();
    private List<String> headers;
    private HashMap<String, Double> headerWeights = new HashMap<>();
    private String sensitiveAttributeHeader;


    private CGItem createItem(List<String> headers, Double pid, Double loadingTime, Double kwh,
                              Double loadingPotential, Double sensitiveAttribute) {
        HashMap<String, Double> data = new HashMap<>();
        data.put("pid", pid);
        data.put("loading_time", loadingTime);
        data.put("loading_potential", loadingPotential);
        data.put("kwH", kwh);
        data.put("building_type", sensitiveAttribute);

        return new CGItem("1", data, new HashMap<>(), headers,
            "station");
    }

    @Before
    public void setUp() {
        this.headers = Arrays.asList("pid", "loading_time", "kwH", "loading_potential");
        for (String header : this.headers) {
            this.headerWeights.put(header, 1.0);
        }
        this.sensitiveAttributeHeader = "building_type";

        globalRanges.put("loading_time", Range.between(100.0, 20000.0));
        globalRanges.put("kwH", Range.between(0.0, 100.0));
        globalRanges.put("loading_potential", Range.between(0.0, 20000.0));
        globalRanges.put("pid", Range.between(1.0, 10.0));
        globalRanges.put("building_type", Range.between(1.0, 4.0));
    }

    @Test
    public void merge3of3ClustersTest() {
        // Create dummy clusters
        Cluster c1 = new Cluster(this.headers, this.headerWeights);
        Cluster c2 = new Cluster(this.headers, this.headerWeights);
        Cluster c3 = new Cluster(this.headers, this.headerWeights);


        // Set up dummy data for clusters
        CGItem item1 = createItem(this.headers, 1.0, 7000.0, 44.5, 7000.0, 1.0);
        CGItem item2 = createItem(this.headers, 2.0, 8000.0, 55.5, 9000.0, 1.0);
        CGItem item3 = createItem(this.headers, 3.0, 9000.0, 66.5, 9000.0, 1.0);

        CGItem item4 = createItem(this.headers, 4.0, 10000.0, 77.5, 12000.0, 2.0);
        CGItem item5 = createItem(this.headers, 5.0, 6000.0, 38.5, 20000.0, 2.0);
        CGItem item6 = createItem(this.headers, 6.0, 7000.0, 44.5, 15000.0, 3.0);
        CGItem item7 = createItem(this.headers, 2.0, 2000.0, 10.5, 9000.0, 1.0);


        List<CGItem> c1_items = new ArrayList<>();
        List<CGItem> c2_items = new ArrayList<>();
        List<CGItem> c3_items = new ArrayList<>();


        c1_items.add(item1);
        c1_items.add(item2);
        c1_items.add(item3);

        c2_items.add(item4);
        c2_items.add(item5);

        c3_items.add(item6);
        c3_items.add(item7);

        // Add items to clusters
        c1.addContents(c1_items);
        c2.addContents(c2_items);
        c3.addContents(c3_items);

        // set up ClusterManagement
        ClusterManagement cm = new ClusterManagement(4, 3, 2, this.headers, sensitiveAttributeHeader);


        // set up Big Gamma (list of non-ks-cluster)
        cm.addToNonAnonymizedClusters(c1);
        cm.addToNonAnonymizedClusters(c2);
        cm.addToNonAnonymizedClusters(c3);


        // make sure clusters aren't already good
        for (Cluster c : Arrays.asList(c1, c2, c3)) {
            assertTrue(c.getContents().size() < 4);
            assertTrue(c.getDiversitySize() < 3);
        }

        Cluster result = cm.mergeClusters(c1, this.globalRanges);

        // Check if the conditions are met
        assertTrue(result.getContents().size() >= 4);
        assertTrue(result.getDiversity().size() >= 3);
    }


    @Test
    public void splitLTest1() {
        // Create dummy clusters
        HashMap<String, Double> weights = new HashMap<>();
        headers.forEach(header -> weights.put(header, 1.0));
        Cluster c1 = new Cluster(this.headers, weights);

        // Set up dummy data for clusters
        CGItem item1 = createItem(this.headers, 1.0, 7000.0, 44.5, 7000.0, 1.0);
        CGItem item2 = createItem(this.headers, 2.0, 8000.0, 55.5, 9000.0, 1.0);
        CGItem item3 = createItem(this.headers, 3.0, 9000.0, 66.5, 9000.0, 1.0);

        CGItem item4 = createItem(this.headers, 4.0, 10000.0, 77.5, 12000.0, 2.0);
        CGItem item5 = createItem(this.headers, 5.0, 6000.0, 38.5, 20000.0, 2.0);
        CGItem item6 = createItem(this.headers, 6.0, 7000.0, 44.5, 15000.0, 3.0);
        CGItem item7 = createItem(this.headers, 2.0, 2000.0, 10.5, 9000.0, 1.0);

        List<CGItem> c1_items = new ArrayList<>();

        c1_items.add(item1);
        c1_items.add(item2);
        c1_items.add(item3);
        c1_items.add(item4);
        c1_items.add(item5);
        c1_items.add(item6);
        c1_items.add(item7);

        // Add items to clusters
        c1.addContents(c1_items);

        int k = 3;
        int l = 3;

        ClusterManagement cm = new ClusterManagement(k, l, 2, this.headers, sensitiveAttributeHeader);
        List<Cluster> splitted = cm.splitL(c1, headers, globalRanges);


        int a = 0;
        int b = 0;
        for (Cluster splitC : splitted) {
            if (splitC.getKSize() < k) {
                a = 1;
            }
            if (splitC.getDiversity().size() < l) {
                b = 1;
            }
        }

        assertTrue(a == 0 && b == 0);
    }

    @Test
    public void splitLTest2() {
        // Create dummy clusters
        HashMap<String, Double> weights = new HashMap<>();
        this.headers.forEach(header -> weights.put(header, 1.0));
        Cluster c1 = new Cluster(this.headers, weights);

        // Set up dummy data for clusters
        CGItem item1 = createItem(this.headers, 2.0, 1000.0, 44.5, 7000.0, 0.0);
        CGItem item2 = createItem(this.headers, 3.0, 1000.0, 44.5, 7000.0, 0.0);
        CGItem item3 = createItem(this.headers, 3.0, 1000.0, 44.5, 7000.0, 0.0);
        CGItem item4 = createItem(this.headers, 1.0, 1000.0, 44.5, 7000.0, 0.0);
        CGItem item5 = createItem(this.headers, 1.0, 1000.0, 44.5, 7000.0, 0.0);
        CGItem item6 = createItem(this.headers, 4.0, 1000.0, 44.5, 7000.0, 0.0);
        CGItem item7 = createItem(this.headers, 1.0, 1000.0, 44.5, 7000.0, 0.0);
        CGItem item8 = createItem(this.headers, 1.0, 1000.0, 44.5, 7000.0, 0.0);
        CGItem item9 = createItem(this.headers, 5.0, 1000.0, 44.5, 7000.0, 0.0);
        CGItem item10 = createItem(this.headers, 1.0, 1000.0, 44.5, 7000.0, 0.0);


        List<CGItem> c1_items = new ArrayList<>();

        c1_items.add(item1);
        c1_items.add(item2);
        c1_items.add(item3);
        c1_items.add(item4);
        c1_items.add(item5);
        c1_items.add(item6);
        c1_items.add(item7);
        c1_items.add(item8);
        c1_items.add(item9);
        c1_items.add(item10);


        // Add items to clusters
        c1.addContents(c1_items);

        int k = 3;
        int l = 1;

        ClusterManagement cm = new ClusterManagement(k, l, 2, this.headers, sensitiveAttributeHeader);
        List<Cluster> splitted = cm.splitL(c1, headers, globalRanges);

        int a = 0;
        int b = 0;
        for (Cluster splitC : splitted) {
            if (splitC.getKSize() < k) {
                a = 1;
            }
            if (splitC.getDiversity().size() < l) {
                b = 1;
            }
        }

        assertTrue(a == 0 && b == 0);
    }

    @Test
    public void splitLTest3() {
        // Create dummy clusters
        HashMap<String, Double> weights = new HashMap<>();
        this.headers.forEach(header -> weights.put(header, 1.0));
        Cluster c1 = new Cluster(this.headers, weights);

        // Set up dummy data for clusters
        CGItem item1 = createItem(this.headers, 1.0, 7000.0, 44.5, 7000.0, 0.0);
        CGItem item2 = createItem(this.headers, 2.0, 8000.0, 55.5, 9000.0, 0.0);
        CGItem item3 = createItem(this.headers, 3.0, 9000.0, 66.5, 9000.0, 0.0);
        CGItem item4 = createItem(this.headers, 4.0, 10000.0, 77.5, 12000.0, 0.0);
        CGItem item5 = createItem(this.headers, 5.0, 6000.0, 38.5, 20000.0, 0.0);
        CGItem item6 = createItem(this.headers, 6.0, 7000.0, 44.5, 15000.0, 0.0);
        CGItem item7 = createItem(this.headers, 7.0, 2000.0, 10.5, 9000.0, 0.0);
        CGItem item8 = createItem(this.headers, 8.0, 2300.0, 30.5, 9000.0, 0.0);
        CGItem item9 = createItem(this.headers, 9.0, 5000.0, 20.5, 9000.0, 0.0);
        CGItem item10 = createItem(this.headers, 10.0, 1000.0, 17.5, 9000.0, 0.0);
        CGItem item11 = createItem(this.headers, 1.0, 1000.0, 17.5, 9000.0, 1.0);
        CGItem item12 = createItem(this.headers, 2.0, 1000.0, 17.5, 9000.0, 1.0);
        CGItem item13 = createItem(this.headers, 3.0, 1000.0, 17.5, 9000.0, 1.0);
        CGItem item14 = createItem(this.headers, 4.0, 1000.0, 17.5, 9000.0, 1.0);
        CGItem item15 = createItem(this.headers, 5.0, 1000.0, 17.5, 9000.0, 1.0);
        CGItem item16 = createItem(this.headers, 6.0, 1000.0, 17.5, 9000.0, 1.0);
        CGItem item17 = createItem(this.headers, 7.0, 1000.0, 17.5, 9000.0, 1.0);
        CGItem item18 = createItem(this.headers, 8.0, 1000.0, 17.5, 9000.0, 1.0);
        CGItem item19 = createItem(this.headers, 9.0, 1000.0, 17.5, 9000.0, 1.0);
        CGItem item20 = createItem(this.headers, 10.0, 1000.0, 17.5, 9000.0, 1.0);


        List<CGItem> c1_items = new ArrayList<>();

        c1_items.add(item1);
        c1_items.add(item2);
        c1_items.add(item3);
        c1_items.add(item4);
        c1_items.add(item5);
        c1_items.add(item6);
        c1_items.add(item7);
        c1_items.add(item8);
        c1_items.add(item9);
        c1_items.add(item10);
        c1_items.add(item11);
        c1_items.add(item12);
        c1_items.add(item13);
        c1_items.add(item14);
        c1_items.add(item15);
        c1_items.add(item16);
        c1_items.add(item17);
        c1_items.add(item18);
        c1_items.add(item19);
        c1_items.add(item20);

        Collections.shuffle(c1_items);
        // Add items to clusters
        c1.addContents(c1_items);

        int k = 3;
        int l = 2;

        ClusterManagement cm = new ClusterManagement(3, 2, 2, this.headers, sensitiveAttributeHeader);
        List<Cluster> splitted = cm.splitL(c1, headers, globalRanges);

        int a = 0;
        int b = 0;
        for (Cluster splitC : splitted) {
            if (splitC.getKSize() < k) {
                a = 1;
            }
            if (splitC.getDiversity().size() < l) {
                b = 1;
            }
        }

        assertTrue(a == 0 && b == 0);
    }


    private List<Double> splitLTestSetup(int k, int l) {
        // Create dummy clusters
        HashMap<String, Double> weights = new HashMap<>();
        this.headers.forEach(header -> weights.put(header, 1.0));
        Cluster c1 = new Cluster(this.headers, weights);

        // Set up dummy data for clusters
        CGItem item1 = createItem(this.headers, 1.0, 7000.0, 44.5, 7000.0, 0.0);
        CGItem item2 = createItem(this.headers, 2.0, 8000.0, 55.5, 9000.0, 0.0);
        CGItem item3 = createItem(this.headers, 3.0, 9000.0, 66.5, 9000.0, 0.0);
        CGItem item4 = createItem(this.headers, 4.0, 10000.0, 77.5, 12000.0, 0.0);
        CGItem item5 = createItem(this.headers, 5.0, 6000.0, 38.5, 20000.0, 0.0);
        CGItem item6 = createItem(this.headers, 6.0, 7000.0, 44.5, 15000.0, 0.0);
        CGItem item7 = createItem(this.headers, 7.0, 2000.0, 10.5, 9000.0, 0.0);
        CGItem item8 = createItem(this.headers, 8.0, 2300.0, 30.5, 9000.0, 0.0);
        CGItem item9 = createItem(this.headers, 9.0, 5000.0, 20.5, 9000.0, 0.0);
        CGItem item10 = createItem(this.headers, 10.0, 1000.0, 7.5, 9000.0, 0.0);
        CGItem item11 = createItem(this.headers, 1.0, 1000.0, 10.5, 9000.0, 1.0);
        CGItem item12 = createItem(this.headers, 2.0, 1000.0, 13.5, 9000.0, 1.0);
        CGItem item13 = createItem(this.headers, 3.0, 1000.0, 16.5, 9000.0, 1.0);
        CGItem item14 = createItem(this.headers, 4.0, 1000.0, 11.5, 9000.0, 1.0);
        CGItem item15 = createItem(this.headers, 5.0, 1000.0, 19.5, 9000.0, 1.0);
        CGItem item16 = createItem(this.headers, 6.0, 1342.0, 9.5, 9000.0, 1.0);
        CGItem item17 = createItem(this.headers, 7.0, 1230.0, 16.5, 9000.0, 1.0);
        CGItem item18 = createItem(this.headers, 6.0, 1120.0, 15.5, 9000.0, 1.0);
        CGItem item19 = createItem(this.headers, 7.0, 1020.0, 17.5, 9000.0, 1.0);
        CGItem item20 = createItem(this.headers, 5.0, 1030.0, 17.5, 9000.0, 1.0);
        CGItem item21 = createItem(this.headers, 2.0, 1030.0, 17.5, 9000.0, 1.0);
        CGItem item22 = createItem(this.headers, 15.0, 1300.0, 17.5, 9000.0, 1.0);
        CGItem item23 = createItem(this.headers, 14.0, 1500.0, 17.5, 9000.0, 1.0);
        CGItem item24 = createItem(this.headers, 13.0, 1200.0, 37.5, 9000.0, 1.0);
        CGItem item25 = createItem(this.headers, 13.0, 1400.0, 57.5, 9000.0, 1.0);
        CGItem item26 = createItem(this.headers, 12.0, 7700.0, 44.5, 15000.0, 0.0);
        CGItem item27 = createItem(this.headers, 12.0, 2800.0, 10.5, 9000.0, 0.0);
        CGItem item28 = createItem(this.headers, 12.0, 2600.0, 30.5, 9000.0, 3.0);
        CGItem item29 = createItem(this.headers, 12.0, 5500.0, 20.5, 9000.0, 3.0);
        CGItem item30 = createItem(this.headers, 11.0, 1200.0, 17.5, 9000.0, 3.0);

        List<CGItem> c1_items = new ArrayList<>();

        c1_items.add(item1);
        c1_items.add(item2);
        c1_items.add(item3);
        c1_items.add(item4);
        c1_items.add(item5);
        c1_items.add(item6);
        c1_items.add(item7);
        c1_items.add(item8);
        c1_items.add(item9);
        c1_items.add(item10);
        c1_items.add(item11);
        c1_items.add(item12);
        c1_items.add(item13);
        c1_items.add(item14);
        c1_items.add(item15);
        c1_items.add(item16);
        c1_items.add(item17);
        c1_items.add(item18);
        c1_items.add(item19);
        c1_items.add(item20);
        c1_items.add(item21);
        c1_items.add(item22);
        c1_items.add(item23);
        c1_items.add(item24);
        c1_items.add(item25);
        c1_items.add(item26);
        c1_items.add(item27);
        c1_items.add(item28);
        c1_items.add(item29);
        c1_items.add(item30);


        Collections.shuffle(c1_items);
        // Add items to clusters
        c1.addContents(c1_items);

        Double currentClusterInfoLoss = c1.informationLoss(globalRanges);

        ClusterManagement cm = new ClusterManagement(k, l, 2, this.headers, sensitiveAttributeHeader);
        List<Cluster> splitted = cm.splitL(c1, headers, globalRanges);

        int a = 0;
        int b = 0;


        List<Double> improvement = new ArrayList<>();
        List<Double> infoLosses = new ArrayList<>();

        for (Cluster splitC : splitted) {
            if (splitC.getKSize() < k) {
                a = 1;
            }
            if (splitC.getDiversity().size() < l) {
                b = 1;
            }
            if (splitC.getKSize() >= k && splitC.getDiversity().size() >= l && splitted.size() != 1) {
                improvement.add(currentClusterInfoLoss - splitC.informationLoss(this.globalRanges));
                infoLosses.add(splitC.informationLoss(this.globalRanges));
            }

        }

        Double avgImprovement = 0.0;
        Double avgNewInfoLoss = splitted.get(0).informationLoss(this.globalRanges);
        if (improvement.size() != 0) {
            avgNewInfoLoss = infoLosses.stream().reduce(0.0, Double::sum) / infoLosses.size();
            avgImprovement = improvement.stream().reduce(0.0, Double::sum) / improvement.size();
        }

        return Arrays.asList((double) splitted.size(), (double) a, (double) b, avgImprovement,
            avgNewInfoLoss);
    }


    /***
     * Experimental Benchmark Test for splitL-Method
     * IGNORE THIS TEST
     ***/
    @Ignore("This Test helps to benchmark the splitL-Method")
    @Test
    public void splitLBenchmarkTest() {
        // Select Parameters
        int times = 150;
        List<Integer> kSizes = Arrays.asList(2, 3, 4, 5, 6, 7, 8, 9, 10);
        int lMAX = 3;

        for (int l = 1; l <= lMAX; l++) {
            for (int k : kSizes) {
                System.out.println("START RUN WITH K: " + k + ", L: " + l);

                float avgImprovement = 0;
                float avgNewInfoLoss = 0;
                int optimalSplitCount = 0;
                int correctKSizeCount = 0;
                int correctDiversityCount = 0;

                for (int i = 0; i < times; i++) {
                    List<Double> res = splitLTestSetup(k, l);
                    if (res.get(0) >= 2) {
                        optimalSplitCount++;
                    }
                    if (res.get(1) == 0) {
                        correctKSizeCount++;
                    }
                    if (res.get(2) == 0) {
                        correctDiversityCount++;
                    }
                    if (res.get(3) != 0) {
                        avgImprovement += res.get(3);
                    }
                    avgNewInfoLoss += res.get(4);

                }

                float avgImproPercentage = avgImprovement / times;
                float avgNewInfoLossPercentage = avgNewInfoLoss / times;
                double splitPercentage = (double) (optimalSplitCount / times) * 100;
                double kPercentage = (double) (correctKSizeCount / times) * 100;
                double lPercentage = (double) (correctDiversityCount / times) * 100;

                System.out.println("Improved Clustering " + splitPercentage + " of the times.");
                System.out.println("Correctly splitted k size " + kPercentage + " of the times.");
                System.out.println("Correctly splitted diversity " + lPercentage + " of the times.");
                System.out.println("Average improvement: " + avgImproPercentage);
                System.out.println("Average new info loss: " + avgNewInfoLossPercentage);
                System.out.println("---------------------------------------------------");
            }
        }

        assertTrue(true);
    }


}
