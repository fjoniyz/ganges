package com.ganges.lib.castleguard;


import java.util.*;
import org.apache.commons.lang3.Range;
import org.apache.commons.math3.distribution.LaplaceDistribution;
import org.apache.commons.math3.random.JDKRandomGenerator;

public class CastleGuard {
    private final CGConfig config;
    private Map<String, Range<Double>> headers;
    private String sensitiveAttr;
    private Deque<Item> items;

    public CastleGuard(CGConfig config, List<String> headers, String sensitiveAttr) {
        this.config = config;
        for (String header : headers) {
            this.headers.put(header, Range.between(null, null));
        }
        this.sensitiveAttr = sensitiveAttr;
    }

    /**
     * insert() in castle.py
     * @param data value tuple
     */
    public void insertData(HashMap<String, Double> data) {
        Random rand = new Random();
        if (config.isUseDiffPrivacy() && rand.nextDouble(1) > config.getBigBeta()) {
            return;
        }
        Item item = new Item(data, new ArrayList<>(this.headers.keySet()), this.sensitiveAttr);
        updateGlobalRanges(item);

        if (config.isUseDiffPrivacy()) {
            perturb(item);
        }
        Optional<Object> cluster = bestSelection(item);
        if (!cluster.isPresent()) {
            // TODO: create cluster here
        } else {
            insertIntoCluster(item, cluster.get());
        }

        items.add(item);
        if (items.size() > config.getDelta()) {
            delayConstraint(items.pop());
        }
    }

    /**
     * Called by the object that manages anonymization class
     * @param item
     * @return
     */
    public Object outputCluster(Item item) {
        return null;
    }

    private void insertIntoCluster(Item item, Object cluster) {
        // TODO: Implement (basis)

    }

    private void createCluster() {
        // TODO: Implement
    }

    private void delayConstraint(Item item) {
        // TODO: Implement
    }

    /**
     * fudge_tuple() in castle.py
     */
    private void perturb(Item item) {
        HashMap<String, Double> data = item.getData();

        for (Map.Entry<String, Range<Double>> header : this.headers.entrySet()) {
            // Check if header has a range
            if (header.getValue().getMinimum() != null || header.getValue().getMaximum() != null) {
                double max_value = header.getValue().getMaximum();
                double min_value = header.getValue().getMinimum();

                // Calaculate scale
                double scale = Math.max((max_value - min_value), 1) / this.config.getPhi();
                
                // Draw random noise from Laplace distribution
                JDKRandomGenerator rg = new JDKRandomGenerator();
                LaplaceDistribution laplaceDistribution = new LaplaceDistribution(rg, 0, scale);
                double noise = laplaceDistribution.sample();

                // Add noise to original value
                double original_value = data.get(header.getKey());
                double perturbed_value = original_value + noise;
                item.updateAttributes(header.getKey(), perturbed_value);
            }
        }
    }

    /**
     * best_selection() in castle.py
     * @return cluster or null
     */
    private Optional<Object> bestSelection(Item item) {
        // TODO: Implement
        return null;
    }

    private void updateGlobalRanges(Item item) {
        // TODO: Implement (Basis)
    }

}
