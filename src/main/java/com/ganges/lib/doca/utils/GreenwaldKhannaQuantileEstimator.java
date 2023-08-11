package myapps.utils;

import java.util.*;

/***
 * Implementation of the Greenwald-Khanna Quantile Estimator algorithm for estimating quantiles in a data stream.
 * Source: https://aakinshin.net/posts/greenwald-khanna-quantile-estimator/
 * or https://github.com/AndreyAkinshin/aakinshin.net/blob/master/content/en/posts/2021/11/greenwald-khanna-quantile-estimator/index.md
 * Migrated to Java
 ***/
public class GreenwaldKhannaQuantileEstimator {
    private static class Tuple {
        public static final Comparator<Tuple> COMPARATOR = Comparator.comparingDouble(Tuple::getValue);

        private final double value; // Observation v[i]
        private int gap; // g[i] = rMin(v[i]) - rMin(v[i - 1])
        private int delta; // delta[i] = rMax(v[i]) - rMin(v[i])
        private List<Double> item;

        public Tuple(double value, int gap, int delta, List<Double> item) {
            this.value = value;
            this.gap = gap;
            this.delta = delta;
            this.item = item;
        }

        public double getValue() {
            return value;
        }

        public int getGap() {
            return gap;
        }

        public int getDelta() {
            return delta;
        }

        public List<Double> getItem() {
            return item;
        }
    }

    private List<Tuple> tuples;
    private int compressingInterval;
    private final double epsilon;
    private int observedElements;

    /***
     *
     * @param epsilon - desired accuracy of the quantile estimation, the smaller the epsilon,
     *                the more accurate the estimation
     */
    public GreenwaldKhannaQuantileEstimator(double epsilon) {
        this.epsilon = epsilon;
        this.compressingInterval = (int) Math.floor(1.0 / (2.0 * this.epsilon));
        this.tuples = new ArrayList<>();
        this.observedElements = 0;
    }

    public List<Tuple> getTuples() {
        return tuples;
    }


    public List<List<Double>> getDomain() {
        List<List<Double>> domain = new ArrayList<>();
        for (Tuple t : tuples) {
            domain.add(t.getItem());
        }
        return domain;
    }

    public List<List<Double>> getDomain(int from, int to) {
        List<List<Double>> domain = new ArrayList<>();
        for (Tuple t : tuples.subList(from, to)) {
            domain.add(t.getItem());
        }
        return domain;
    }

    public void add(double[] v, List<List<Double>> item) {
        for (int i = 0; i < v.length; i++) {
            add(v[i], item.get(i));
        }
    }

    public void add(double v) {
        Tuple t = new Tuple(v, 1, (int) Math.floor(2.0 * this.epsilon * observedElements), null);
        int i = getInsertIndex(t);
        if (i == 0 || i == this.tuples.size()) {
            t.delta = 0;
        }

        this.tuples.add(i, t);
        this.observedElements++;

        if (this.observedElements % this.compressingInterval == 0) {
            compress();
        }
    }

    public void add(double v, List<Double> item) {
        Tuple t = new Tuple(v, 1, (int) Math.floor(2.0 * this.epsilon * observedElements), item);
        int i = getInsertIndex(t);
        if (i == 0 || i == this.tuples.size()) {
            t.delta = 0;
        }

        this.tuples.add(i, t);
        this.observedElements++;

        if (this.observedElements % this.compressingInterval == 0) {
            compress();
        }
    }

    private int getInsertIndex(Tuple v) {
        int index = binarySearch(this.tuples, v, Tuple.COMPARATOR);
        return index >= 0 ? index : ~index;
    }

    private static <T> int binarySearch(List<? extends T> list, T key, Comparator<? super T> c) {
        int low = 0;
        int high = list.size() - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            T midVal = list.get(mid);
            int cmp = c.compare(midVal, key);

            if (cmp < 0)
                low = mid + 1;
            else if (cmp > 0)
                high = mid - 1;
            else
                return mid; // key found
        }
        return -(low + 1); // key not found
    }

    public HashMap<Integer, Double> getQuantile(double p) {
        if (this.tuples.isEmpty()) {
            throw new IllegalStateException("Sequence contains no elements");
        }

        double rank = p * (this.observedElements - 1) + 1;
        int margin = (int) Math.ceil(this.epsilon * this.observedElements);

        int bestIndex = -1;
        double bestDist = Double.MAX_VALUE;
        int rMin = 0;
        for (int i = 0; i < this.tuples.size(); i++) {
            Tuple t = this.tuples.get(i);
            rMin += t.getGap();
            int rMax = rMin + t.getDelta();
            if (rank - margin <= rMin && rMax <= rank + margin) {
                double currentDist = Math.abs(rank - (rMin + rMax) / 2.0);
                if (currentDist < bestDist) {
                    bestDist = currentDist;
                    bestIndex = i;
                }
            }
        }
        if (bestIndex == -1) {
            throw new IllegalStateException("Failed to find the requested quantile");
        }

        HashMap<Integer, Double> resultMap = new HashMap<>();
        resultMap.put(bestIndex, this.tuples.get(bestIndex).getValue());
        //resultMap.put("index", (double) bestIndex);
        return resultMap;
        //return Arrays.asList(this.tuples.get(bestIndex).getValue(), (double) bestIndex);
    }

    public void compress() {
        for (int i = this.tuples.size() - 2; i >= 1; i--) {
            while (i < this.tuples.size() - 1 && deleteIfNeeded(i)) {
                // Do nothing, continue deleting
            }
        }
    }

    private boolean deleteIfNeeded(int i) {
        Tuple t1 = this.tuples.get(i);
        Tuple t2 = this.tuples.get(i + 1);
        int threshold = (int) Math.floor(2.0 * this.epsilon * this.observedElements);
        if (t1.getDelta() >= t2.getDelta() && t1.getGap() + t2.getGap() + t2.getDelta() < threshold) {
            this.tuples.remove(i);
            t2.gap += t1.getGap();
            return true;
        }
        return false;
    }
}