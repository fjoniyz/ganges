package serdes;

public interface AnonymizedMessage {
  public String getId();
  public Double[] getValuesListByKeys(String[] keys);

}
