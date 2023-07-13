package serdes;

public interface AnonymizedMessage {
  public String getId();
  public double[] getValuesListFromKeys(String[] keys);

}
