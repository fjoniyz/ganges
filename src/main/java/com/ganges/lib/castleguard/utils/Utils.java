package com.ganges.lib.castleguard.utils;

import org.apache.commons.lang3.Range;

public class Utils {
    Range<Integer> updateRange() {
        return null;
    }

    public float range_information_loss(Range<Float> actual, Range<Float> other){
        float diff_self = Math.abs(actual.getMaximum()-actual.getMinimum());
        float diff_other = Math.abs(other.getMaximum()-other.getMinimum());;
        if (diff_other == 0){
            return 0F;
        }
        return diff_self/diff_other;
    }

    public float range_difference(Float range){
        return Math.abs(range.getMaximum() -range.getMinimum());
    }
}
