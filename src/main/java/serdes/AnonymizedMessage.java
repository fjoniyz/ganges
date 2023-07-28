package serdes;

public interface AnonymizedMessage {
  public String getId();

  String getTimestamp();

  double[] getValuesListFromKeys(String[] keys);

    public Double[] getValuesListByKeys(String[] keys);

}
