package com.ganges.lib.castleguard;

import com.ganges.lib.AbstractCluster;
import com.ganges.lib.AbstractItem;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CGItem extends AbstractItem {

  private final Double sensitiveAttr;
  private Cluster parent;
  private Double pid;


  public CGItem(String externalId, Map<String, Double> data,
                Map<String, String> nonAnonymizedData, List<String> headers,
                String sensitiveAttr) {
    super(externalId, data, nonAnonymizedData, headers);
    this.sensitiveAttr = data.get(sensitiveAttr);
    this.pid = data.get("pid");
  }

  public Double getPid() {
    return this.pid;
  }
  public Double getSensitiveAttr() {
    return sensitiveAttr;
  }

    public Cluster getCluster() {
        return parent;
    }
    public void setCluster(Cluster cluster) {
    this.parent = cluster;
  }
}
