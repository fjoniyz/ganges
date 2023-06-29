package com.ganges.lib.castleguard.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.commons.lang3.Range;

public class Utils {
  private static Random random;

  public Utils() {
    this.random = new Random();
  }

  public static Range<Float> updateRange(Range<Float> range, float newVal) {
    float max = Math.max(range.getMaximum(), newVal);
    float min = Math.min(range.getMinimum(), newVal);
    Range<Float> newRange = Range.between(min, max);
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

  public float rangeInformationLoss(Range<Float> actual, Range<Float> other) {
    float diff_self = Math.abs(actual.getMaximum() - actual.getMinimum());
    float diff_other = Math.abs(other.getMaximum() - other.getMinimum());
    if (diff_other == 0) {
      return 0F;
    }
    return diff_self / diff_other;
  }

  /**
   * @param range: Range Object with Floats
   * @return: the maximum difference within Range object
   */
  public float rangeDifference(Range<Float> range) {
    return Math.abs(range.getMaximum() - range.getMinimum());
  }
}
