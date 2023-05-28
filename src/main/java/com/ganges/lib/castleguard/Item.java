package com.ganges.lib.castleguard;

import java.util.HashMap;
import java.util.List;

public class Item {
    private HashMap<String, Integer> data;
    private List<String> headers;
    private String sensitiveAttr;
    private Object cluster;

    public Item(HashMap<String, Integer> data, List<String> headers, String sensitiveAttr) {
        this.data = data;
        this.headers = headers;
        this.sensitiveAttr = sensitiveAttr;
    }

    public HashMap<String, Integer> getData() {
        return data;
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
