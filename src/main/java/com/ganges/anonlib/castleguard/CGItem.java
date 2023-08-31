package com.ganges.anonlib.castleguard;

import com.ganges.anonlib.AbstractItem;
import java.util.List;
import java.util.Map;
import lombok.Getter;

public class CGItem extends AbstractItem {

  @Getter
  private final Double sensitiveAttr;
  private CGCluster parent;
  @Getter
  private final Double pid;


  public CGItem(String externalId, Map<String, Double> data,
                Map<String, String> nonAnonymizedData, List<String> headers,
                String sensitiveAttr) {
    super(externalId, data, nonAnonymizedData, headers);
    this.sensitiveAttr = data.get(sensitiveAttr);
    this.pid = data.get("pid");
  }

  public CGCluster getCluster() {
    return parent;
  }

  public void setCluster(CGCluster CGCluster) {
    this.parent = CGCluster;
  }
}
