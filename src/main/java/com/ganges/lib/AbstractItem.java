package com.ganges.lib;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class AbstractItem {

  protected final String externalId;
  protected final Map<String, Float> data;
  protected final Map<String, String> nonAnonymizedData;
  protected final List<String> headers;
  protected AbstractCluster parent;


  public AbstractItem(String externalId, Map<String, Float> data,
                      Map<String, String> nonAnonymizedData, List<String> headers) {
    this.externalId = externalId;
    this.data = data;
    this.nonAnonymizedData = nonAnonymizedData;
    this.headers = new ArrayList<>(headers);
    
  }

  public Map<String, Float> getData() {
    return data;
  }
  public List<String> getHeaders() {
    return headers;
  }
  public AbstractCluster getCluster() {
    return parent;
  }
  public void setCluster(AbstractCluster cluster) {
    this.parent = cluster;
  }
  public Map<String, String> getNonAnonymizedData() {
    return nonAnonymizedData;
  }
  public void removeData(String elem) {
    this.data.remove(elem);
  }
  public void updateAttributes(String header, Float value) {
    this.data.put(header, value);
  }
  public void addHeaders(String elem) {
    this.headers.add(elem);
  }
  public void removeHeaders(String elem) {
    this.headers.remove(elem);
  }
  public String getExternalId() {
    return externalId;
  }
}
