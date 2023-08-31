package com.ganges.anonlib.doca;

import com.ganges.anonlib.AbstractItem;
import java.util.List;
import java.util.Map;

public class DocaItem extends AbstractItem {

  private DocaCluster parent;

  public DocaItem(String externalId, Map<String, Double> data,
                  Map<String, String> nonAnonymizedData, List<String> headers) {
    super(externalId, data, nonAnonymizedData, headers);
  }

  public DocaCluster getCluster() {
    return parent;
  }

  public void setCluster(DocaCluster cluster) {
    this.parent = cluster;
  }


}
