package com.ganges.lib.castleguard;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.checkerframework.checker.nullness.qual.NonNull;

public class CGItem {
    private final Map<String, Float> data;
    private final Map<String, String> nonAnonymizedData;
    private final List<String> headers;
    private final Float sensitiveAttr;
    private Cluster parent;
    private Float pid;
    private String externalId;
    public CGItem(String externalId, Map<String, Float> data,
                  Map<String, String> nonAnonymizedData, List<String> headers,
                  String sensitiveAttr) {
        this.externalId = externalId;
        this.data = data;
        this.nonAnonymizedData = nonAnonymizedData;
        this.headers = new ArrayList<>(headers);
        this.sensitiveAttr = data.get(sensitiveAttr);
        this.pid = data.get("pid");
    }

    public CGItem(Map<String, Float> data, List<String> headers,
                  String sensitiveAttr) {
        this.externalId = "";
        this.data = data;
        this.nonAnonymizedData = new HashMap<>();
        this.headers = new ArrayList<>(headers);
        this.sensitiveAttr = data.get(sensitiveAttr);
        this.pid = data.get("pid");
    }

    public Float getPid() {
        return this.pid;
    }

    public Map<String, Float> getData() {
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

    public Float tupleDistance(CGItem item) {
        // TODO: Implement
        return null;
    }

    public String getExternalId() {
        return externalId;
    }

    public Map<String, String> getNonAnonymizedData() {
        return nonAnonymizedData;
    }
}
