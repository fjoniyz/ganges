package com.ganges.lib.castleguard;


import java.util.*;

public class CastleGuard {
    private final CGConfig config;
    private List<String> headers;
    private String sensitiveAttr;
    private Deque<Item> items;

    public CastleGuard(CGConfig config, List<String> headers, String sensitiveAttr) {
        this.config = config;
        this.headers = headers;
        this.sensitiveAttr = sensitiveAttr;
    }

    /**
     * insert() in castle.py
     * @param data value tuple
     */
    public void insertData(HashMap<String, Integer> data) {
        Random rand = new Random();
        if (config.isUseDiffPrivacy() && rand.nextDouble(1) > config.getBigBeta()) {
            return;
        }
        Item item = new Item(data, this.headers, this.sensitiveAttr);
        updateGlobalRanges(item);

        if (config.isUseDiffPrivacy()) {
            item = perturb(item);
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
     * @return
     */
    private Item perturb(Item item) {
        // TODO: Implement
        return null;
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
