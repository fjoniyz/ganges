package myapps;

import java.util.Map;
import java.util.Objects;

public class AnonymizationItem {
  private final String id;
  private final Map<String, Double> values;

  public AnonymizationItem(String id, Map<String, Double> values) {
    this.id = id;
    this.values = values;
  }

  public Map<String, Double> getValues() {
    return values;
  }

  public String getId() {
    return id;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    AnonymizationItem item = (AnonymizationItem) o;
    return Objects.equals(id, item.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id);
  }
}
