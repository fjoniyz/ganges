package com.ganges.lib.castleguard;

import org.apache.commons.lang3.Range;

import java.util.HashMap;
import java.util.List;
import java.util.Optional;

public class CastleGuard {
    private CGConfig config;
    private List<String> headers;
    private String sensitiveAttr;

    public CastleGuard(CGConfig config, List<String> headers, String sensitiveAttr) {
        this.config = config;
        this.headers = headers;
        this.sensitiveAttr = sensitiveAttr;
    }

    /**
     * insert() in castle.py
     * @param data
     */
    public void insertData(HashMap<String, Integer> data) {
        // TODO: Implement (basis)
        // Create Item here
    }

    /**
     * Called by the object that manages anonymization class
     * @param item
     * @return
     */
    public Object outputCluster(Item item) {
        return null;
    }

    private void insertIntoCluster(HashMap<String, Integer> data, Object cluster) {
        // TODO: Implement (basis)
    }

    private void createCluster() {
        // TODO: Implement
    }

    private void delayConstraint(HashMap<String, Integer> data) {
        // TODO: Implement
    }

    /**
     * fudge_tuple() in castle.py
     * @return
     */
    private HashMap<String, Integer> perturb() {
        // TODO: Implement
        return null;
    }

    /**
     * best_selection() in castle.py
     * @return cluster
     */
    private Optional<Object> bestSelection() {
        // TODO: Implement
        return null;
    }

    private void updateGlobalRanges(Object data) {
        // TODO: Implement (Basis)
    }

}
