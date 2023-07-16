package serdes;

public interface AnonymizedMessage {
  String getId();

  Double[] getValuesListByKeys(String[] keys);

}
