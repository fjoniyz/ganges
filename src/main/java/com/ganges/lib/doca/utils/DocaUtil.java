package com.ganges.lib.doca.utils;

import com.ganges.lib.castleguard.CGItem;
import com.ganges.lib.doca.DocaItem;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.Range;

public class DocaUtil {

  private static int currentID = 0;

  /**
   * Performs element-wise division of two arrays of doubles.
   * If the corresponding element in the divisor array is zero, the result will be set to zero
   *
   * @param a the first array of doubles
   * @param b the second array of doubles
   * @return the division of the two arrays
   */
  public static HashMap<String, Double> divisionWith0(HashMap<String, Double> a,
                                                     HashMap<String, Double> b) {
    HashMap<String, Double> result = new HashMap<>();
    for (Map.Entry<String, Double> aEntry : a.entrySet()) {
      result.put(aEntry.getKey(),
          (b.get(aEntry.getKey()) != 0) ? aEntry.getValue() / b.get(aEntry.getKey()) : 0);
    }
    return result;
  }

  /**
   * Returns the maximum value in a 2D array of doubles.
   *
   * @param X the 2D array of doubles
   * @return the maximum value in the array
   */
  public static Map<String, Double> getMax(List<DocaItem> X) {
    Map<String, Double> maxMap = new HashMap<>();

    for (String header : X.get(0).getHeaders()) {
      double max = Double.NEGATIVE_INFINITY;
      for (DocaItem item : X) {
        max = Math.max(max, item.getData().get(header));
      }
      maxMap.put(header, max);
    }
    return maxMap;
  }

  /**
   * Returns the minimum value in a 2D array of doubles.
   *
   * @param X the 2D array of doubles
   * @return the minimum value in the array
   */
  public static Map<String, Double> getMin(List<DocaItem> X) {
    Map<String, Double> minMap = new HashMap<>();

    for (String header : X.get(0).getHeaders()) {
      double min = Double.POSITIVE_INFINITY;
      for (DocaItem item : X) {
        min = Math.min(min, item.getData().get(header));
      }
      minMap.put(header, min);
    }
    return minMap;
  }

  /**
   * Calculates the sum of elements in an array of doubles.
   *
   * @param values the array of doubles
   * @return the sum of elements in the array
   */
  public static double getSumOfElementsInArray(double[] values) {
    double sum = 0;
    for (double value : values) {
      sum += value;
    }
    return sum;
  }

  /**
   * Converts a list of data points to a list of items.
   *
   * @param dataSet the list of data points to be converted
   * @return a list of items created from the data points
   */
  public static List<CGItem> dataPointsToItems(List<List<Double>> dataSet) {
    List<CGItem> items = new ArrayList<>();
    int i = 0;
    for (List<Double> dataPoint : dataSet) {
      int attrIndex = 0;
      HashMap<String, Float> attributeValue = new HashMap<>();
      for (double attr : dataPoint) {
        attributeValue.put(String.valueOf(attrIndex), (float) attr);
        attrIndex++;
      }
      CGItem item = new CGItem(String.valueOf(currentID++), attributeValue, null, new ArrayList<>(attributeValue.keySet()), String.valueOf(i));
      items.add(item);
      i++;
    }
    return items;
  }

  /**
   * Calculates the standard deviation of a list of numbers
   *
   * @param numbers list of numbers from which the standard deviation should be calculated
   * @return standard deviation of the list of numbers
   */
  public static double calculateStandardDeviation(List<Double> numbers) {
    int size = numbers.size();
    if (size == 0) {
      throw new IllegalArgumentException("List cannot be empty.");
    }

    double mean = numbers.stream()
        .mapToDouble(Double::doubleValue)
        .average()
        .orElse(0.0);

    double sumOfSquaredDifferences = numbers.stream()
        .mapToDouble(number -> Math.pow(number - mean, 2))
        .sum();

    double meanOfSquaredDifferences = sumOfSquaredDifferences / size;

    return Math.sqrt(meanOfSquaredDifferences);
  }

  /**
   * Get the difference between the minimum and maximum value of each attribute
   *
   * @return HashMap with the attribute name as key and the difference as value
   */
  public static HashMap<String, Double> getAttributeDiff(HashMap<String, Range<Double>> rangeMap) {
    HashMap<String, Double> dif = new HashMap<>();
    for (Map.Entry<String, Range<Double>> attributeRange : rangeMap.entrySet()) {
      dif.put(attributeRange.getKey(),
          attributeRange.getValue().getMaximum() - attributeRange.getValue().getMinimum());
    }
    return dif;
  }


}
