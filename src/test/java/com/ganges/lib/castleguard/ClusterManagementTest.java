package com.ganges.lib.castleguard;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.Range;

import com.ganges.lib.castleguard.*;
import com.ganges.lib.castleguard.utils.ClusterManagement;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Assert;
import org.junit.Test;



public class ClusterManagementTest {
    
    private Cluster cluster;
    private HashMap<String, Range<Float>> globalRanges = new HashMap<>();
    private List<String> headers;
    private String sensitiveAttributeHeader;



    private Item createItem(List<String> headers, Float sessionId, Float loadingTime, Float kwh, Float loadingPotential, Float sensitiveAttribute){
        HashMap<String, Float> data = new HashMap<>();
        data.put("session_id", sessionId);
        data.put("loading_time", loadingTime);
        data.put("loading_potential", loadingPotential);
        data.put("kwH", kwh);
        data.put("building_type", sensitiveAttribute);
        
        return new Item(data, headers, this.sensitiveAttributeHeader);
    }

    @Before
    public void setUp() {
        this.headers = Arrays.asList("session_id", "loading_time", "kwH", "loading_potential");
        this.sensitiveAttributeHeader = "building_type";
        cluster = new Cluster(headers);

        globalRanges.put("loading_time", Range.between(100F, 20000F));
        globalRanges.put("kwH", Range.between(0F, 100F));
        globalRanges.put("loading_potential", Range.between(0F, 20000F));
        globalRanges.put("session_id", Range.between(1F, 10F));
        globalRanges.put("building_type", Range.between(1F, 4F));
    }

    @Test
    public void merge3of3ClustersTest() {
        // Create dummy clusters
        Cluster c1 = new Cluster(this.headers);
        Cluster c2 = new Cluster(this.headers);
        Cluster c3 = new Cluster(this.headers);


        // Set up dummy data for clusters
        Item item1 = createItem(this.headers, 1F, 7000F, 44.5F,  7000F, 1F);
        Item item2 = createItem(this.headers, 2F, 8000F, 55.5F,  9000F, 1F);
        Item item3 = createItem(this.headers, 3F, 9000F, 66.5F,  9000F, 1F);

        Item item4 = createItem(this.headers, 4F, 10000F, 77.5F,  12000F, 2F);
        Item item5 = createItem(this.headers, 5F, 6000F, 38.5F,  20000F, 2F);
        Item item6 = createItem(this.headers, 6F, 7000F, 44.5F,  15000F, 3F);
        Item item7 = createItem(this.headers, 2F, 2000F, 10.5F,  9000F, 1F);


        List<Item> c1_items = new ArrayList<>();
        List<Item> c2_items = new ArrayList<>();
        List<Item> c3_items = new ArrayList<>();
        

        c1_items.add(item1);
        c1_items.add(item2);
        c1_items.add(item3);

        c2_items.add(item4);
        c2_items.add(item5);

        c3_items.add(item6);
        c3_items.add(item7);

        // Add items to clusters
        c1.setContents(c1_items);
        c2.setContents(c2_items);
        c3.setContents(c3_items);

        // set up ClusterManagement
        ClusterManagement cm = new ClusterManagement(4, 3, 2, this.headers, sensitiveAttributeHeader);


        // set up Big Gamma (list of non-ks-cluster)
        cm.addToBigGamma(c1);
        cm.addToBigGamma(c2);
        cm.addToBigGamma(c3);


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


    @Ignore
    public void merge2of3ClustersTest() {
        // Create dummy clusters
        Cluster c1 = new Cluster(this.headers);
        Cluster c2 = new Cluster(this.headers);
        Cluster c3 = new Cluster(this.headers);


        // Set up dummy data for clusters
        Item item1 = createItem(this.headers, 1F, 7000F, 44.5F,  7000F, 1F);
        Item item2 = createItem(this.headers, 2F, 8000F, 55.5F,  9000F, 1F);
        //Item item3 = createItem(this.headers, 3F, 9000F, 66.5F,  9000F, 1F);

        Item item4 = createItem(this.headers, 4F, 10000F, 77.5F,  12000F, 2F);
        Item item5 = createItem(this.headers, 5F, 6000F, 38.5F,  20000F, 2F);
        Item item6 = createItem(this.headers, 6F, 7000F, 44.5F,  15000F, 3F);
        Item item7 = createItem(this.headers, 2F, 2000F, 10.5F,  9000F, 2F);


        List<Item> c1_items = new ArrayList<>();
        List<Item> c2_items = new ArrayList<>();
        List<Item> c3_items = new ArrayList<>();
        

        c1_items.add(item1);
        c1_items.add(item2);
        //c1_items.add(item3);

        c2_items.add(item4);
        c2_items.add(item5);

        c3_items.add(item6);
        c3_items.add(item7);

        // Add items to clusters
        c1.setContents(c1_items);
        c2.setContents(c2_items);
        c3.setContents(c3_items);

        // set up ClusterManagement
        ClusterManagement cm = new ClusterManagement(3, 3, 2, this.headers, sensitiveAttributeHeader);


        // set up Big Gamma (list of non-ks-cluster)
        cm.addToBigGamma(c1);
        cm.addToBigGamma(c2);
        cm.addToBigGamma(c3);


        // make sure clusters aren't already good
        for (Cluster c : Arrays.asList(c1, c2, c3)) {
            assertTrue(c.getContents().size() < 3);
            assertTrue(c.getDiversitySize() < 3);
        }

        Cluster result = cm.mergeClusters(c1, this.globalRanges);

        // Check if the conditions are met
        assertTrue(result.getContents().size() >= 3);
        assertTrue(result.getContents().size() <= 4);

        assertTrue(result.getDiversity().size() >= 3); 
    }

    @Test
    public void splitLTest() {
        // Create dummy clusters
        Cluster c1 = new Cluster(this.headers);
        //Cluster c2 = new Cluster(this.headers);
        //Cluster c3 = new Cluster(this.headers);


        // Set up dummy data for clusters
        Item item1 = createItem(this.headers, 1F, 7000F, 44.5F,  7000F, 1F);
        Item item2 = createItem(this.headers, 2F, 8000F, 55.5F,  9000F, 1F);
        Item item3 = createItem(this.headers, 3F, 9000F, 66.5F,  9000F, 1F);

        Item item4 = createItem(this.headers, 4F, 10000F, 77.5F,  12000F, 2F);
        Item item5 = createItem(this.headers, 5F, 6000F, 38.5F,  20000F, 2F);
        Item item6 = createItem(this.headers, 6F, 7000F, 44.5F,  15000F, 3F);
        Item item7 = createItem(this.headers, 2F, 2000F, 10.5F,  9000F, 1F);


        List<Item> c1_items = new ArrayList<>();
        //List<Item> c2_items = new ArrayList<>();
        //List<Item> c3_items = new ArrayList<>();
        

        c1_items.add(item1);
        c1_items.add(item2);
        c1_items.add(item3);

        c1_items.add(item4);
        c1_items.add(item5);

        c1_items.add(item6);
        c1_items.add(item7);

        // Add items to clusters
        c1.setContents(c1_items);
        //c2.setContents(c2_items);
        //c3.setContents(c3_items);

        ClusterManagement cm = new ClusterManagement(3, 3, 2, this.headers, sensitiveAttributeHeader);
        cm.splitL(c1, headers, globalRanges);

        assert(true);
    }

    @Test
    public void generateBucketsTest() {
        assert(false);
    }


}
