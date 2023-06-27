package myapps;

import java.util.ArrayList;
import java.util.List;

import myapps.util.GreenwaldKhannaQuantileEstimator;

public class Doca {

    private double eps;
    private double delta;
    private int delay_constraint;
    private int beta;
    private boolean inplace;
    private List<Double> Vinf;
    private List<Double> Vsup;

    // number of tuples to be evaluated for domain
    private int m;

    private List<List<Double>> domain;
    private List<List<Double>> stableDomain;
    private GreenwaldKhannaQuantileEstimator GKQuantileEstimator;
    private double GKQError = 0.02;


    public Doca(double eps, double delta, int delay_constraint, int beta, boolean inplace) {
        this.eps = eps;
        this.delta = delta;
        this.delay_constraint = delay_constraint;
        this.m = delay_constraint;
        this.beta = beta;
        this.inplace = inplace;
        this.domain = new ArrayList<>();
        this.stableDomain = new ArrayList<>();
        this.GKQuantileEstimator = new GreenwaldKhannaQuantileEstimator(eps);
    }

    public static double getMax(double[][] X) {
        double max = Double.NEGATIVE_INFINITY;
        for (double[] row : X) {
            for (double value : row) {
                max = Math.max(max, value);
            }
        }
        return max;
    }

    public static double getMin(double[][] X) {
        double min = Double.POSITIVE_INFINITY;
        for (double[] row : X) {
            for (double value : row) {
                min = Math.min(min, value);
            }
        }
        return min;
    }

    /**
     * @param a,b arrays of double which need to get divided element by element(have to be the same length)
     * @return an array which includes either 0 or a[i]/b[i]
     */
    public static double[] divisionWith0(double[] a, double[] b) {
        double[] result = new double[a.length];
        for (int i = 0; i < a.length; i++) {
            result[i] = (b[i] != 0) ? a[i] / b[i] : 0;
        }
        return result;
    }

    public static double getSumOfElementsInArray(double[] values) {
        double sum = 0;
        for (double value : values) {
            sum += value;
        }
        return sum;
    }

    public void addTuple(double[][] X) {
        boolean isStable = addToDomain(X);
        if (isStable) {
            docaPhase();
        }
    }

    private void docaPhase() {
        doca(this.stableDomain, false);
    }

    private boolean addToDomain(double[][] X) {
        // Add new Tuples to Domain D
        for (double[] row : X) {
            List<Double> tuple = new ArrayList<>();
            double sum = 0;
            for (double value : row) {
                tuple.add(value);
                sum += value;
            }
            this.domain.add(tuple);

            // Add tuple to GKQEstimator
            // EXPERIMENTAL: Use mean of tuple as value for GK
            double mean = sum / row.length;
            this.GKQuantileEstimator.add(mean);

            // Add Quantile to Vinf and Vsup
            double quantilVinf = (1.0 - this.delta)/2.0;
            double quantilVsup = (1.0 + this.delta)/2.0;
            this.Vinf.add(this.GKQuantileEstimator.getQuantile(quantilVinf));
            this.Vsup.add(this.GKQuantileEstimator.getQuantile(quantilVsup));

            // Check if domain is big enough
            if (Vinf.size() > m && Vsup.size() > m) {

                double stdVinf = calculateStandardDeviation(Vinf);
                double stdVsup = calculateStandardDeviation(Vsup);

                double meanVinf = Vinf.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
                double meanVsup = Vsup.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);

                //coefficient of variation
                double cvVinf = stdVinf / meanVinf;
                double cvVsup = stdVsup / meanVsup;

                // check if domain is stable
                if (cvVinf < this.delta && cvVsup < this.delta) {
                    this.stableDomain = this.domain;

                    this.domain = new ArrayList<>();
                    this.Vinf = new ArrayList<>();
                    this.Vsup = new ArrayList<>();
                    return true;
                }
            }
        }
        return false;
    }

    private double calculateStandardDeviation(List<Double> numbers) {
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

    public static double[][] doca(
            double[][] X, double eps, int delay_constraint, int beta, boolean inplace) {
        int num_instances = X.length;
        int num_attributes = X[0].length;

        double sensitivity = Math.abs((getMax(X) - getMin(X)));

        List<List<Integer>> clusters = new ArrayList<>();
        List<List<Integer>> clusters_final = new ArrayList<>();

        // Cluster attribute minimum/maximum
        List<double[]> mn_c = new ArrayList<>();
        List<double[]> mx_c = new ArrayList<>();

        // Global Attribute Minimum/Maximum
        double[] mn = new double[num_attributes];
        double[] mx = new double[num_attributes];
        for (int i = 0; i < num_attributes; i++) {
            mn[i] = Double.POSITIVE_INFINITY;
            mx[i] = Double.NEGATIVE_INFINITY;
        }

        // Losses saved for tau
        List<Double> losses = new ArrayList<>();

        double tau = 0;

        // Create Output structure
        double[][] output;
        if (inplace) {
            output = X;
        } else {
            output = new double[num_instances][num_attributes];
        }

        int TODOREMOVE_Perfect = 0;

        for (int clock = 0; clock < num_instances; clock++) {
            if (clock % 1000 == 0) {
                System.out.println("Clock " + clock + " " + TODOREMOVE_Perfect);
            }

            double[] data_point = X[clock];

            // Update min/max
            for (int i = 0; i < num_attributes; i++) {
                mn[i] = Math.min(mn[i], data_point[i]);
                mx[i] = Math.max(mx[i], data_point[i]);
            }

            double[] dif = new double[num_attributes];
            for (int i = 0; i < num_attributes; i++) {
                dif[i] = mx[i] - mn[i];
            }

            // Find best Cluster
            Integer best_cluster = null;
            if (!clusters.isEmpty()) {
                // Calculate enlargement (the value is not yet divided by the number of attributes!)
                double[] enlargement = new double[clusters.size()];
                for (int c = 0; c < clusters.size(); c++) {
                    double sum = 0;
                    for (int i = 0; i < num_attributes; i++) {
                        sum +=
                                Math.max(0, data_point[i] - mx_c.get(c)[i])
                                        - Math.min(0, data_point[i] - mn_c.get(c)[i]);
                    }
                    enlargement[c] = sum;
                }

                double min_enlarge = Double.MAX_VALUE;

                List<Integer> ok_clusters = new ArrayList<>();
                List<Integer> min_clusters = new ArrayList<>();

                for (int c = 0; c < clusters.size(); c++) {
                    double enl = enlargement[c];
                    if (enl == min_enlarge) {
                        min_clusters.add(c);
                        double overall_loss = (enl + getSumOfElementsInArray(divisionWith0(mx_c.get(c), dif))) / num_attributes;
                        if (overall_loss <= tau) {
                            ok_clusters.add(c);
                        }
                    }
                }

                if (!ok_clusters.isEmpty()) {
                    TODOREMOVE_Perfect += 1;
                    best_cluster =
                            ok_clusters.stream()
                                    .min(
                                            (c1, c2) -> Integer.compare(clusters.get(c1).size(), clusters.get(c2).size()))
                                    .orElse(null);
                } else if (clusters.size() >= beta) {
                    best_cluster =
                            min_clusters.stream()
                                    .min(
                                            (c1, c2) -> Integer.compare(clusters.get(c1).size(), clusters.get(c2).size()))
                                    .orElse(null);
                }
            }

            if (best_cluster == null) {
                // Add new Cluster
                List<Integer> new_cluster = new ArrayList<>();
                new_cluster.add(clock);
                clusters.add(new_cluster);

                // Set Min/Max of new Cluster
                mn_c.add(data_point.clone());
                mx_c.add(data_point.clone());
            } else {
                clusters.get(best_cluster).add(clock);
                // Update min/max
                double[] mn_cluster = mn_c.get(best_cluster);
                double[] mx_cluster = mx_c.get(best_cluster);
                mn_cluster[best_cluster] = Math.min(mn_cluster[best_cluster], data_point[best_cluster]);
                mx_cluster[best_cluster] = Math.max(mx_cluster[best_cluster], data_point[best_cluster]);
            }

            List<Integer> overripe_clusters = new ArrayList<>();
            for (int c = 0; c < clusters.size(); c++) {
                if (clusters.get(c).contains(clock - delay_constraint)) {
                    overripe_clusters.add(c);
                }
            }
            assert overripe_clusters.size() <= 1
                    : "Every datapoint should only be able to be in one cluster!?";
            if (!overripe_clusters.isEmpty()) {
                int c = overripe_clusters.get(0);
                double[] dif_cluster = new double[num_attributes];
                for (int i = 0; i < num_attributes; i++) {
                    dif_cluster[i] = mx_c.get(c)[i] - mn_c.get(c)[i];
                }
                double loss = getSumOfElementsInArray(divisionWith0(dif_cluster, dif)) / num_attributes;
                losses.add(loss);
                clusters_final.add(clusters.get(c));
                clusters.remove(c);
                mn_c.remove(c);
                mx_c.remove(c);
            }
        }

        clusters_final.addAll(clusters);

        for (List<Integer> cs : clusters_final) {
            double[] mean = new double[num_attributes];
            for (int i : cs) {
                for (int j = 0; j < num_attributes; j++) {
                    mean[j] += X[i][j];
                }
            }
            for (int j = 0; j < num_attributes; j++) {
                mean[j] /= cs.size();
            }
            double scale = (sensitivity / (cs.size() * eps));
            double[] laplace = new double[num_attributes];
            for (int j = 0; j < num_attributes; j++) {
                laplace[j] = Math.random() - 0.5;
            }
            for (int j = 0; j < num_attributes; j++) {
                output[cs.get(0)][j] = mean[j] + scale * laplace[j];
            }
        }

        return output;
    }

    private double[][] doca(List<List<Double>> X, boolean inplace) {
        int num_attributes = X.get(0).size();
        int num_instances = X.size();

        double[][] dataArr = X.stream()
                .map(row -> row.stream()
                        .mapToDouble(Double::doubleValue)
                        .toArray())
                .toArray(double[][]::new);

        double sensitivity = Math.abs((getMax(dataArr) - getMin(dataArr)));

        List<List<Integer>> clusters = new ArrayList<>();
        List<List<Integer>> clusters_final = new ArrayList<>();

        // Cluster attribute minimum/maximum
        List<double[]> mn_c = new ArrayList<>();
        List<double[]> mx_c = new ArrayList<>();

        // Global Attribute Minimum/Maximum
        double[] mn = new double[num_attributes];
        double[] mx = new double[num_attributes];
        for (int i = 0; i < num_attributes; i++) {
            mn[i] = Double.POSITIVE_INFINITY;
            mx[i] = Double.NEGATIVE_INFINITY;
        }

        // Losses saved for tau
        List<Double> losses = new ArrayList<>();

        double tau = 0;

        // Create Output structure
        double[][] output;
        if (inplace) {
            output = dataArr;
        } else {
            output = new double[num_instances][num_attributes];
        }

        int TODOREMOVE_Perfect = 0;

        for (int clock = 0; clock < num_instances; clock++) {
            if (clock % 1000 == 0) {
                System.out.println("Clock " + clock + " " + TODOREMOVE_Perfect);
            }

            double[] data_point = dataArr[clock];

            // Update min/max
            for (int i = 0; i < num_attributes; i++) {
                mn[i] = Math.min(mn[i], data_point[i]);
                mx[i] = Math.max(mx[i], data_point[i]);
            }

            double[] dif = new double[num_attributes];
            for (int i = 0; i < num_attributes; i++) {
                dif[i] = mx[i] - mn[i];
            }

            // Find best Cluster
            Integer best_cluster = null;
            if (!clusters.isEmpty()) {
                // Calculate enlargement (the value is not yet divided by the number of attributes!)
                double[] enlargement = new double[clusters.size()];
                for (int c = 0; c < clusters.size(); c++) {
                    double sum = 0;
                    for (int i = 0; i < num_attributes; i++) {
                        sum +=
                                Math.max(0, data_point[i] - mx_c.get(c)[i])
                                        - Math.min(0, data_point[i] - mn_c.get(c)[i]);
                    }
                    enlargement[c] = sum;
                }

                double min_enlarge = Double.MAX_VALUE;

                List<Integer> ok_clusters = new ArrayList<>();
                List<Integer> min_clusters = new ArrayList<>();

                for (int c = 0; c < clusters.size(); c++) {
                    double enl = enlargement[c];
                    if (enl == min_enlarge) {
                        min_clusters.add(c);
                        double overall_loss = (enl + getSumOfElementsInArray(divisionWith0(mx_c.get(c), dif))) / num_attributes;
                        if (overall_loss <= tau) {
                            ok_clusters.add(c);
                        }
                    }
                }

                if (!ok_clusters.isEmpty()) {
                    TODOREMOVE_Perfect += 1;
                    best_cluster =
                            ok_clusters.stream()
                                    .min(
                                            (c1, c2) -> Integer.compare(clusters.get(c1).size(), clusters.get(c2).size()))
                                    .orElse(null);
                } else if (clusters.size() >= beta) {
                    best_cluster =
                            min_clusters.stream()
                                    .min(
                                            (c1, c2) -> Integer.compare(clusters.get(c1).size(), clusters.get(c2).size()))
                                    .orElse(null);
                }
            }

            if (best_cluster == null) {
                // Add new Cluster
                List<Integer> new_cluster = new ArrayList<>();
                new_cluster.add(clock);
                clusters.add(new_cluster);

                // Set Min/Max of new Cluster
                mn_c.add(data_point.clone());
                mx_c.add(data_point.clone());
            } else {
                clusters.get(best_cluster).add(clock);
                // Update min/max
                double[] mn_cluster = mn_c.get(best_cluster);
                double[] mx_cluster = mx_c.get(best_cluster);
                mn_cluster[best_cluster] = Math.min(mn_cluster[best_cluster], data_point[best_cluster]);
                mx_cluster[best_cluster] = Math.max(mx_cluster[best_cluster], data_point[best_cluster]);
            }

            List<Integer> overripe_clusters = new ArrayList<>();
            for (int c = 0; c < clusters.size(); c++) {
                if (clusters.get(c).contains(clock - delay_constraint)) {
                    overripe_clusters.add(c);
                }
            }
            assert overripe_clusters.size() <= 1
                    : "Every datapoint should only be able to be in one cluster!?";
            if (!overripe_clusters.isEmpty()) {
                int c = overripe_clusters.get(0);
                double[] dif_cluster = new double[num_attributes];
                for (int i = 0; i < num_attributes; i++) {
                    dif_cluster[i] = mx_c.get(c)[i] - mn_c.get(c)[i];
                }
                double loss = getSumOfElementsInArray(divisionWith0(dif_cluster, dif)) / num_attributes;
                losses.add(loss);
                clusters_final.add(clusters.get(c));
                clusters.remove(c);
                mn_c.remove(c);
                mx_c.remove(c);
            }
        }

        clusters_final.addAll(clusters);

        for (List<Integer> cs : clusters_final) {
            double[] mean = new double[num_attributes];
            for (int i : cs) {
                for (int j = 0; j < num_attributes; j++) {
                    mean[j] += dataArr[i][j];
                }
            }
            for (int j = 0; j < num_attributes; j++) {
                mean[j] /= cs.size();
            }
            double scale = (sensitivity / (cs.size() * eps));
            double[] laplace = new double[num_attributes];
            for (int j = 0; j < num_attributes; j++) {
                laplace[j] = Math.random() - 0.5;
            }
            for (int j = 0; j < num_attributes; j++) {
                output[cs.get(0)][j] = mean[j] + scale * laplace[j];
            }
        }

        return output;
    }
}
