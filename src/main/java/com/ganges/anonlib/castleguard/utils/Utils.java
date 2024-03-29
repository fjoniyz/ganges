package com.ganges.anonlib.castleguard.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.commons.lang3.Range;

public class Utils {
  private static Random random;

  public Utils() {
    this.random = new Random();
  }

  public static Range<Double> updateRange(Range<Double> range, Double newVal) {
    Double max = Math.max(range.getMaximum(), newVal);
    Double min = Math.min(range.getMinimum(), newVal);
    Range<Double> newRange = Range.between(min, max);
    return newRange;
  }

  /** Replacement for non-existant python function np.random.choice() */
  public static <T> T randomChoice(List<T> content) {
    return randomChoice(content, 1).get(0);
  }

  /**
   * @param content: List of items
   * @param size
   * @return random Element in the List of Items
   */
  public static <T> List<T> randomChoice(List<T> content, int size) {
    List<T> sampled = new ArrayList<>();
    int i = 0;
    while (i < size) {
      int index = random.nextInt(content.size());
      if (sampled.contains(content.get(index))) {
        continue;
      }
      sampled.add(content.get(index));
      i++;
    }

    return sampled;
  }

  public Double rangeInformationLoss(Range<Double> actual, Range<Double> other, Double weight) {
    Double diff_self = Math.abs(actual.getMaximum() - actual.getMinimum());
    Double diff_other = Math.abs(other.getMaximum() - other.getMinimum());
    if (diff_other == 0) {
      return 0.0;
    }
    return weight * (diff_self / diff_other);
  }

  /**
   * @param range: Range Object with Floats
   * @return: the maximum difference within Range object
   */
  public Double rangeDifference(Range<Double> range) {
    return Math.abs(range.getMaximum() - range.getMinimum());
  }
}
