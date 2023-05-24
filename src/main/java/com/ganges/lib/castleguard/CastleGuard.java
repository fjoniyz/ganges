package com.ganges.lib.castleguard;

import org.apache.commons.lang3.Range;

import java.util.HashMap;
import java.util.List;
import java.util.Optional;

public class CastleGuard {

    public CastleGuard(CGConfig config, List<String> headers, String sensitiveAttr) {
    }

    /**
     * insert() in castle.py
     * @param data
     */
    void insertData(HashMap<String, Integer> data) {
        // TODO: Implement (basis)
        // Create Item here
    }

    Object outputCluster(Item item) {
        return null;
    }

    void insertIntoCluster(HashMap<String, Integer> data, Object cluster) {
        // TODO: Implement (basis)
    }

    void createCluster() {
        // TODO: Implement
    }

    void delayConstraint(HashMap<String, Integer> data) {
        // TODO: Implement
    }

    /**
     * fudge_tuple() in castle.py
     * @return
     */
    HashMap<String, Integer> perturb() {
        // TODO: Implement
        return null;
    }

    /**
     * best_selection() in castle.py
     * @return cluster
     */
    Optional<Object> bestSelection() {
        // TODO: Implement
        return null;
    }

    void updateGlobalRanges(Object data) {
        // TODO: Implement (Basis)
    }

}
