package com.ganges.lib.castleguard;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.checkerframework.checker.nullness.qual.NonNull;

public class Item {
    private HashMap<String, Float> data;
    private List<String> headers;
    private Float sensitiveAttr;
    private Cluster parent;

    public Item(HashMap<String, Float> data, List<String> headers, String sensitiveAttr) {
        this.data = data;
        this.headers = new ArrayList<>(headers);
        this.sensitiveAttr = data.get(sensitiveAttr);
    }

    public Item(@NonNull Item another) {
        this.data = (HashMap<String, Float>) another.data.clone();
        this.headers = new ArrayList<>(another.getHeaders());
        this.sensitiveAttr = another.sensitiveAttr;
        this.parent = another.parent;
    }

    public HashMap<String, Float> getData() {
        return data;
    }


    public void removeData(String elem) {
        this.data.remove(elem);
    }

    public void updateAttributes(String header, Float value) {
        this.data.put(header, value);
    }

    public List<String> getHeaders() {
        return headers;
    }

    public void addHeaders(String elem) {
        this.headers.add(elem);
    }

    public void removeHeaders(String elem) {
        this.headers.remove(elem);
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
