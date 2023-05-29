package com.ganges.lib.castleguard;

import java.util.HashMap;
import java.util.List;

public class Item {
    private HashMap<String, Float> data;
    private List<String> headers;
    private Float sensitiveAttr;
    private Cluster parent;

    public Item(HashMap<String, Float> data, List<String> headers, Float sensitiveAttr) {
        this.data = data;
        this.headers = headers;
        this.sensitiveAttr = sensitiveAttr;
    }

    public HashMap<String, Float> getData() {
        return data;
    }

    public List<String> getHeaders() {
        return headers;
    }

    public Float getSensitiveAttr() {
        return sensitiveAttr;
    }

    public Cluster getCluster() {
        return parent;
    }

    public void setCluster(Cluster cluster) {
        this.parent = cluster;
    }

    public Float tupleDistance(Item item) {
        // TODO: Implement
        return null;
    }
}
