package com.ganges.lib.doca;

import com.ganges.lib.castleguard.Cluster;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DocaItem {
  private final Map<String, Double> data;
  private final Map<String, String> nonAnonymizedData;
  private final List<String> headers;
  private DocaCluster parent;
  private String externalId;

  public DocaItem(String externalId, Map<String, Double> data,
                Map<String, String> nonAnonymizedData, List<String> headers) {
    this.externalId = externalId;
    this.data = data;
    this.nonAnonymizedData = nonAnonymizedData;
    this.headers = new ArrayList<>(headers);
  }

  public Map<String, Double> getData() {
    return data;
  }

  public void removeData(String elem) {
    this.data.remove(elem);
  }

  public void updateAttributes(String header, Double value) {
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

  public DocaCluster getCluster() {
    return parent;
  }

  public void setCluster(DocaCluster cluster) {
    this.parent = cluster;
  }

  public String getExternalId() {
    return externalId;
  }

  public Map<String, String> getNonAnonymizedData() {
    return nonAnonymizedData;
  }
}
