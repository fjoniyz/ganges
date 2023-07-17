package serdes;

public interface AnonymizedMessage {
  public String getId();
  public String getTimestamp();
  public double[] getValuesListFromKeys(String[] keys);

}
