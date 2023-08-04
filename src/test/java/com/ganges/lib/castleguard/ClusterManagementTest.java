package com.ganges.lib.castleguard;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Collections;

import org.apache.commons.lang3.Range;

import com.ganges.lib.castleguard.utils.ClusterManagement;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;


public class ClusterManagementTest {

    private final HashMap<String, Range<Float>> globalRanges = new HashMap<>();
    private List<String> headers;
    private String sensitiveAttributeHeader;


    private CGItem createItem(List<String> headers, Float pid, Float loadingTime, Float kwh, Float loadingPotential, Float sensitiveAttribute) {
        HashMap<String, Float> data = new HashMap<>();
        data.put("pid", pid);
        data.put("loading_time", loadingTime);
        data.put("loading_potential", loadingPotential);
        data.put("kwH", kwh);
        data.put("building_type", sensitiveAttribute);

        return new CGItem(data, headers, this.sensitiveAttributeHeader);
    }

    @Before
    public void setUp() {
        this.headers = Arrays.asList("pid", "loading_time", "kwH", "loading_potential");
        this.sensitiveAttributeHeader = "building_type";

        globalRanges.put("loading_time", Range.between(100F, 20000F));
        globalRanges.put("kwH", Range.between(0F, 100F));
        globalRanges.put("loading_potential", Range.between(0F, 20000F));
        globalRanges.put("pid", Range.between(1F, 10F));
        globalRanges.put("building_type", Range.between(1F, 4F));
    }

    @Test
    public void merge3of3ClustersTest() {
        // Create dummy clusters
        Cluster c1 = new Cluster(this.headers);
        Cluster c2 = new Cluster(this.headers);
        Cluster c3 = new Cluster(this.headers);


        // Set up dummy data for clusters
        CGItem item1 = createItem(this.headers, 1F, 7000F, 44.5F, 7000F, 1F);
        CGItem item2 = createItem(this.headers, 2F, 8000F, 55.5F, 9000F, 1F);
        CGItem item3 = createItem(this.headers, 3F, 9000F, 66.5F, 9000F, 1F);

        CGItem item4 = createItem(this.headers, 4F, 10000F, 77.5F, 12000F, 2F);
        CGItem item5 = createItem(this.headers, 5F, 6000F, 38.5F, 20000F, 2F);
        CGItem item6 = createItem(this.headers, 6F, 7000F, 44.5F, 15000F, 3F);
        CGItem item7 = createItem(this.headers, 2F, 2000F, 10.5F, 9000F, 1F);


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
        Cluster c1 = new Cluster(this.headers);

        // Set up dummy data for clusters
        CGItem item2 = createItem(this.headers, 2F, 8000F, 55.5F, 9000F, 1F);
        CGItem item3 = createItem(this.headers, 3F, 9000F, 66.5F, 9000F, 1F);
        CGItem item1 = createItem(this.headers, 1F, 7000F, 44.5F, 7000F, 1F);
        CGItem item4 = createItem(this.headers, 4F, 10000F, 77.5F, 12000F, 2F);
        CGItem item5 = createItem(this.headers, 5F, 6000F, 38.5F, 20000F, 2F);
        CGItem item6 = createItem(this.headers, 6F, 7000F, 44.5F, 15000F, 3F);
        CGItem item7 = createItem(this.headers, 2F, 2000F, 10.5F, 9000F, 1F);

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
        Cluster c1 = new Cluster(this.headers);

        // Set up dummy data for clusters
        CGItem item1 = createItem(this.headers, 2F, 1000F, 44.5F, 7000F, 0F);
        CGItem item2 = createItem(this.headers, 3F, 1000F, 44.5F, 7000F, 0F);
        CGItem item3 = createItem(this.headers, 3F, 1000F, 44.5F, 7000F, 0F);
        CGItem item4 = createItem(this.headers, 1F, 1000F, 44.5F, 7000F, 0F);
        CGItem item5 = createItem(this.headers, 1F, 1000F, 44.5F, 7000F, 0F);
        CGItem item6 = createItem(this.headers, 4F, 1000F, 44.5F, 7000F, 0F);
        CGItem item7 = createItem(this.headers, 1F, 1000F, 44.5F, 7000F, 0F);
        CGItem item8 = createItem(this.headers, 1F, 1000F, 44.5F, 7000F, 0F);
        CGItem item9 = createItem(this.headers, 5F, 1000F, 44.5F, 7000F, 0F);
        CGItem item10 = createItem(this.headers, 1F, 1000F, 44.5F, 7000F, 0F);


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
        Cluster c1 = new Cluster(this.headers);

        // Set up dummy data for clusters
        CGItem item1 = createItem(this.headers, 1F, 7000F, 44.5F, 7000F, 0F);
        CGItem item2 = createItem(this.headers, 2F, 8000F, 55.5F, 9000F, 0F);
        CGItem item3 = createItem(this.headers, 3F, 9000F, 66.5F, 9000F, 0F);
        CGItem item4 = createItem(this.headers, 4F, 10000F, 77.5F, 12000F, 0F);
        CGItem item5 = createItem(this.headers, 5F, 6000F, 38.5F, 20000F, 0F);
        CGItem item6 = createItem(this.headers, 6F, 7000F, 44.5F, 15000F, 0F);
        CGItem item7 = createItem(this.headers, 7F, 2000F, 10.5F, 9000F, 0F);
        CGItem item8 = createItem(this.headers, 8F, 2300F, 30.5F, 9000F, 0F);
        CGItem item9 = createItem(this.headers, 9F, 5000F, 20.5F, 9000F, 0F);
        CGItem item10 = createItem(this.headers, 10F, 1000F, 17.5F, 9000F, 0F);
        CGItem item11 = createItem(this.headers, 1F, 1000F, 17.5F, 9000F, 1F);
        CGItem item12 = createItem(this.headers, 2F, 1000F, 17.5F, 9000F, 1F);
        CGItem item13 = createItem(this.headers, 3F, 1000F, 17.5F, 9000F, 1F);
        CGItem item14 = createItem(this.headers, 4F, 1000F, 17.5F, 9000F, 1F);
        CGItem item15 = createItem(this.headers, 5F, 1000F, 17.5F, 9000F, 1F);
        CGItem item16 = createItem(this.headers, 6F, 1000F, 17.5F, 9000F, 1F);
        CGItem item17 = createItem(this.headers, 7F, 1000F, 17.5F, 9000F, 1F);
        CGItem item18 = createItem(this.headers, 8F, 1000F, 17.5F, 9000F, 1F);
        CGItem item19 = createItem(this.headers, 9F, 1000F, 17.5F, 9000F, 1F);
        CGItem item20 = createItem(this.headers, 10F, 1000F, 17.5F, 9000F, 1F);


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


    private List<Float> splitLTestSetup(int k, int l) {
        // Create dummy clusters
        Cluster c1 = new Cluster(this.headers);

        // Set up dummy data for clusters
        CGItem item1 = createItem(this.headers, 1F, 7000F, 44.5F, 7000F, 0F);
        CGItem item2 = createItem(this.headers, 2F, 8000F, 55.5F, 9000F, 0F);
        CGItem item3 = createItem(this.headers, 3F, 9000F, 66.5F, 9000F, 0F);
        CGItem item4 = createItem(this.headers, 4F, 10000F, 77.5F, 12000F, 0F);
        CGItem item5 = createItem(this.headers, 5F, 6000F, 38.5F, 20000F, 0F);
        CGItem item6 = createItem(this.headers, 6F, 7000F, 44.5F, 15000F, 0F);
        CGItem item7 = createItem(this.headers, 7F, 2000F, 10.5F, 9000F, 0F);
        CGItem item8 = createItem(this.headers, 8F, 2300F, 30.5F, 9000F, 0F);
        CGItem item9 = createItem(this.headers, 9F, 5000F, 20.5F, 9000F, 0F);
        CGItem item10 = createItem(this.headers, 10F, 1000F, 7.5F, 9000F, 0F);
        CGItem item11 = createItem(this.headers, 1F, 1000F, 10.5F, 9000F, 1F);
        CGItem item12 = createItem(this.headers, 2F, 1000F, 13.5F, 9000F, 1F);
        CGItem item13 = createItem(this.headers, 3F, 1000F, 16.5F, 9000F, 1F);
        CGItem item14 = createItem(this.headers, 4F, 1000F, 11.5F, 9000F, 1F);
        CGItem item15 = createItem(this.headers, 5F, 1000F, 19.5F, 9000F, 1F);
        CGItem item16 = createItem(this.headers, 6F, 1342F, 9.5F, 9000F, 1F);
        CGItem item17 = createItem(this.headers, 7F, 1230F, 16.5F, 9000F, 1F);
        CGItem item18 = createItem(this.headers, 6F, 1120F, 15.5F, 9000F, 1F);
        CGItem item19 = createItem(this.headers, 7F, 1020F, 17.5F, 9000F, 1F);
        CGItem item20 = createItem(this.headers, 5F, 1030F, 17.5F, 9000F, 1F);
        CGItem item21 = createItem(this.headers, 2F, 1030F, 17.5F, 9000F, 1F);
        CGItem item22 = createItem(this.headers, 15F, 1300F, 17.5F, 9000F, 1F);
        CGItem item23 = createItem(this.headers, 14F, 1500F, 17.5F, 9000F, 1F);
        CGItem item24 = createItem(this.headers, 13F, 1200F, 37.5F, 9000F, 1F);
        CGItem item25 = createItem(this.headers, 13F, 1400F, 57.5F, 9000F, 1F);
        CGItem item26 = createItem(this.headers, 12F, 7700F, 44.5F, 15000F, 0F);
        CGItem item27 = createItem(this.headers, 12F, 2800F, 10.5F, 9000F, 0F);
        CGItem item28 = createItem(this.headers, 12F, 2600F, 30.5F, 9000F, 3F);
        CGItem item29 = createItem(this.headers, 12F, 5500F, 20.5F, 9000F, 3F);
        CGItem item30 = createItem(this.headers, 11F, 1200F, 17.5F, 9000F, 3F);

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

        float currentClusterInfoLoss = c1.informationLoss(globalRanges);

        ClusterManagement cm = new ClusterManagement(k, l, 2, this.headers, sensitiveAttributeHeader);
        List<Cluster> splitted = cm.splitL(c1, headers, globalRanges);

        int a = 0;
        int b = 0;


        List<Float> improvement = new ArrayList<>();
        List<Float> infoLosses = new ArrayList<>();

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

        float avgImprovement = 0F;
        float avgNewInfoLoss = splitted.get(0).informationLoss(this.globalRanges);
        if (improvement.size() != 0) {
            avgNewInfoLoss = infoLosses.stream().reduce(0F, Float::sum) / infoLosses.size();
            avgImprovement = improvement.stream().reduce(0F, Float::sum) / improvement.size();
        }

        return Arrays.asList((float) splitted.size(), (float) a, (float) b, avgImprovement, avgNewInfoLoss);
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

                float avgImprovement = 0F;
                float avgNewInfoLoss = 0F;
                int optimalSplitCount = 0;
                int correctKSizeCount = 0;
                int correctDiversityCount = 0;

                for (int i = 0; i < times; i++) {
                    List<Float> res = splitLTestSetup(k, l);
                    if (res.get(0) >= 2) {
                        optimalSplitCount++;
                    }
                    if (res.get(1) == 0) {
                        correctKSizeCount++;
                    }
                    if (res.get(2) == 0) {
                        correctDiversityCount++;
                    }
                    if (res.get(3) != 0F) {
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
