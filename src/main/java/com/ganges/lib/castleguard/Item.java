package com.ganges.lib.castleguard;

import java.util.HashMap;
import java.util.List;

public class Item {
    private HashMap<String, Double> data;
    private List<String> headers;
    private String sensitiveAttr;
    private Object cluster;

    public Item(HashMap<String, Double> data, List<String> headers, String sensitiveAttr) {
        this.data = data;
        this.headers = headers;
        this.sensitiveAttr = sensitiveAttr;
    }

    public HashMap<String, Double> getData() {
        return data;
    }

    public void updateAttributes(String header, Double value) {
        this.data.put(header, value);
    }

    public List<String> getHeaders() {
        return headers;
    }

    public String getSensitiveAttr() {
        return sensitiveAttr;
    }

    public Object getCluster() {
        return cluster;
    }

    public void setCluster(Object cluster) {
        this.cluster = cluster;
    }
}
