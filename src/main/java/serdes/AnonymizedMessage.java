package serdes;

public interface AnonymizedMessage {
  public String getId();
  public String getTimestamp();
  public String getProducerTimestamp();
  public double[] getValuesListFromKeys(String[] keys);

}
