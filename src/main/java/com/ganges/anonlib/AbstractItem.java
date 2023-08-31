package com.ganges.anonlib;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.Getter;

public abstract class AbstractItem {

  @Getter
  protected final String externalId;
  @Getter
  protected final Map<String, Double> data;
  @Getter
  protected final Map<String, String> nonAnonymizedData;
  @Getter
  protected final List<String> headers;
  protected AbstractCluster parent;


  public AbstractItem(String externalId, Map<String, Double> data,
                      Map<String, String> nonAnonymizedData, List<String> headers) {
    this.externalId = externalId;
    this.data = data;
    this.nonAnonymizedData = nonAnonymizedData;
    this.headers = new ArrayList<>(headers);

  }

  public AbstractCluster getCluster() {
    return parent;
  }

  public void setCluster(AbstractCluster cluster) {
    this.parent = cluster;
  }

  public void removeData(String elem) {
    this.data.remove(elem);
  }

  public void updateAttributes(String header, Double value) {
    this.data.put(header, value);
  }

  public void addHeaders(String elem) {
    this.headers.add(elem);
  }

  public void removeHeaders(String elem) {
    this.headers.remove(elem);
  }

}
