package myapps.utils;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;


public class QuantilEstimatorTest {

    @Test
    public void testQuantileEstimator() {
        GreenwaldKhannaQuantileEstimator estimator = new GreenwaldKhannaQuantileEstimator(0.01);

        // Add values to the estimator
        estimator.add(10);
        estimator.add(20);
        estimator.add(30);
        estimator.add(40);
        estimator.add(50);

        // Calculate quantiles and assert their values
        List<Double> quantile25 = estimator.getQuantile(0.25);
        assertEquals(20.0, quantile25.get(0),0.0);

        List<Double> quantile50 = estimator.getQuantile(0.5);
        assertEquals(30.0, quantile50.get(0), 0.0);

        List<Double> quantile75 = estimator.getQuantile(0.75);
        assertEquals(40.0, quantile75.get(0), 0.0);

        // Add more values
        estimator.add(60);
        estimator.add(70);
        estimator.add(80);
        estimator.add(90);
        estimator.add(100);

        // Calculate quantiles again
        quantile25 = estimator.getQuantile(0.25);
        assertEquals(30.0, quantile25.get(0), 0.0);

        quantile50 = estimator.getQuantile(0.5);
        assertEquals(50.0, quantile50.get(0), 0.0);

        quantile75 = estimator.getQuantile(0.75);
        assertEquals(80.0, quantile75.get(0), 0.0);
    }
}
