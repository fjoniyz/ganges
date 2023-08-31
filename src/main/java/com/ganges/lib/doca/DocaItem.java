package com.ganges.lib.doca;

import com.ganges.lib.AbstractItem;
import com.ganges.lib.castleguard.Cluster;
import java.util.ArrayList;
import java.util.HashMap;
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
