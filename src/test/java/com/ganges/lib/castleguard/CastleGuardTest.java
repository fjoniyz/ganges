package com.ganges.lib.castleguard;

import org.apache.commons.lang3.Range;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;

import static org.junit.Assert.*;
public class CastleGuardTest {

    public void preparation(){
        ArrayList<String> headers = new ArrayList<>();
        headers.add("timeseries_id");
        headers.add("SecondsEnergyConsumption");
        headers.add("station");
        Cluster testCluster = new Cluster(headers);

        HashMap<String, Range<Float>> globalRanges = new HashMap<>();
        globalRanges.put(headers.get(0), Range.between(0.0F, 100.0F));
        globalRanges.put(headers.get(1), Range.between(0.0F, 1000.0F));

        HashMap<String, Float> dataOne = new HashMap<>();
        dataOne.put(headers.get(0), 1.0F);
        dataOne.put(headers.get(1), 200.0F);
        dataOne.put(headers.get(2), 5.0F);

        Item one = new Item(dataOne, headers, null);

        CGConfig config = new CGConfig(3, 10, 5, 1, 5, 1, 100 * Math.log(2), true);
        CastleGuard castle = new CastleGuard(config, headers, "station");

        castle.updateGlobalRanges(one);
        Assert.assertEquals(castle.getGlobalRanges(), globalRanges);

    }

    @Test
    public void insertDataTest() {

      }

    @Test
    public void suppressItemTest() {

      }

    @Test
    public void updateGlobalRangesTest() {
        // TODO: create a Cluster then update_global ranges with an item and check this
        ArrayList<String> headers = new ArrayList<>();
        headers.add("timeseries_id");
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

        Item one = new Item(dataOne, headers, "station");

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

        Item two = new Item(dataTwo, headers, "station");
        castle.updateGlobalRanges(two);
        Assert.assertEquals(castle.getGlobalRanges(), newGlobalRanges);
      }
}