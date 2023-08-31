package com.ganges.anonlib.doca.utils;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;


public class QuantilEstimatorTest {

    @Test
    public void testQuantileEstimator() {
        GreenwaldKhannaQuantileEstimator estimator = new GreenwaldKhannaQuantileEstimator(0.01);

        // Add values to the estimator
        estimator.add(10, null);
        estimator.add(20, null);
        estimator.add(30, null);
        estimator.add(40, null);
        estimator.add(50, null);

        // Calculate quantiles and assert their values
        List<Double> quantile25 = new ArrayList<>(estimator.getQuantile(0.25).values());
        assertEquals(20.0, quantile25.get(0),0.0);

        List<Double> quantile50 = new ArrayList<>(estimator.getQuantile(0.5).values());
        assertEquals(30.0, quantile50.get(0), 0.0);

        List<Double> quantile75 = new ArrayList<>(estimator.getQuantile(0.75).values());
        assertEquals(40.0, quantile75.get(0), 0.0);

        // Add more values
        estimator.add(60, null);
        estimator.add(70, null);
        estimator.add(80, null);
        estimator.add(90, null);
        estimator.add(100, null);

        // Calculate quantiles again
        quantile25 = new ArrayList<>(estimator.getQuantile(0.25).values());
        assertEquals(30.0, quantile25.get(0), 0.0);

        quantile50 = new ArrayList<>(estimator.getQuantile(0.5).values());
        assertEquals(50.0, quantile50.get(0), 0.0);

        quantile75 = new ArrayList<>(estimator.getQuantile(0.75).values());
        assertEquals(80.0, quantile75.get(0), 0.0);
    }
}
