package com.ganges.lib.castleguard.utils;

import com.ganges.lib.castleguard.Item;
import org.apache.commons.lang3.Range;

import java.util.List;
import java.util.Random;

public class Utils {
    public static Range<Integer> updateRange() {
        return null;
    }

    Random random;

    public Utils() {
        this.random = new Random();
    }

    public float range_information_loss(Range<Float> actual, Range<Float> other) {
        float diff_self = Math.abs(actual.getMaximum() - actual.getMinimum());
        float diff_other = Math.abs(other.getMaximum() - other.getMinimum());
        ;
        if (diff_other == 0) {
            return 0F;
        }
        return diff_self / diff_other;
    }

    public float range_difference(Range<Float> range) {
        /* Arg: Range Object with Floats
        Return: the maximum difference within Range object
         */
        return Math.abs(range.getMaximum() - range.getMinimum());
    }

    // replacement for nonexistant python function np.random.choice()
    public Item random_choice(List<Item> content) {
        /* Arg: a List of Items
        Return: random Element in the List of Items
         */
        int index = random.nextInt(content.size());
        return content.get(index);
    }
}
