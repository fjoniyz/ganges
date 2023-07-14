package serdes;

public interface AnonymizedMessage {
  public String getId();
  public Double[] getValuesListFromKeys(String[] keys);

}
