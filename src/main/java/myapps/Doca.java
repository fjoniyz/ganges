package myapps;

import java.util.*;

import myapps.util.GreenwaldKhannaQuantileEstimator;

public class Doca {

    private double eps;
    private double delta;
    private int beta;
    private boolean inplace;
    private List<Double> Vinf = new ArrayList<>();
    private List<Double> Vsup = new ArrayList<>();

    // tolerance parameter for domain building
    private final double LAMBDA = 0.15;

    // number of tuples that have to be collected before a phase begins with checking the release requirements
    private int delayConstraint;

    private List<List<Double>> stableDomain;
    private GreenwaldKhannaQuantileEstimator GKQuantileEstimator;
    private final double GKQError = 0.02;

    // could be potentially also just used as lower bound (if domains wont get stable)
    private int sizeOfStableDomain = 200;
    private double tau;


    public Doca(double eps, double delta, int delayConstraint, int beta, boolean inplace) {
        this.eps = eps;
        this.delta = delta;
        this.delayConstraint = delayConstraint;
        this.beta = beta;
        this.inplace = inplace;
        this.stableDomain = new ArrayList<>();
        this.GKQuantileEstimator = new GreenwaldKhannaQuantileEstimator(GKQError);
        this.tau = 0;
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

    /**
     * Added Tuple gets either suppressed or released to the DOCA Phase
     *
     * @param X Input Tuple
     */
    public void addTuple(double[][] X) {
        List<List<Double>> domain = this.addToDomain(X);
        if (domain != null) {
            this.doca(domain, false);
        }
    }

    /**
     * Adds a Tuple to the domain and checks if it is stable.
     * if domain is stable, it will be released
     * @param X Tuple to be added
     * @return the domain if it is stable, otherwise null
     */
    protected List<List<Double>> addToDomain(double[][] X) {
        double tolerance = this.LAMBDA;
        // Add new Tuples to Domain D
        for (double[] row : X) {
            List<Double> tuple = new ArrayList<>();
            double sum = 0;
            for (double value : row) {
                tuple.add(value);
                sum += value;
            }

            // Add tuple to GKQEstimator
            // EXPERIMENTAL: Use mean of tuple as value for GK
            double mean = sum / row.length;
            this.GKQuantileEstimator.add(mean, tuple);

            // Add Quantile to Vinf and Vsup
            List<Double> estQuantilInf = this.GKQuantileEstimator.getQuantile((1.0 - this.delta)/2.0);
            List<Double> estQuantilSup = this.GKQuantileEstimator.getQuantile((1.0 + this.delta)/2.0);

            this.Vinf.add(estQuantilInf.get(0));
            this.Vsup.add(estQuantilSup.get(0));

            // Check if domain is big enough
            if (Vinf.size() == sizeOfStableDomain && Vsup.size() == sizeOfStableDomain) {

                double stdVinf = calculateStandardDeviation(Vinf);
                double stdVsup = calculateStandardDeviation(Vsup);

                double meanVinf = Vinf.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
                double meanVsup = Vsup.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);

                //coefficient of variation
                double cvVinf = stdVinf / meanVinf;
                double cvVsup = stdVsup / meanVsup;

                // check if domain is stable
                if (cvVinf < tolerance && cvVsup < tolerance) {
                    //System.out.println("Domain is stable");
                    //System.out.println("Coefficient of variation Sup: " + cvVsup);
                    //System.out.println("V-Inf: " + this.Vinf);
                    //System.out.println("V-Sup: " + this.Vsup);
                    //System.out.println("Coefficient of variation Inf: " + cvVinf);

                    List<List<Double>> domain = this.GKQuantileEstimator.getDomain();
                    this.stableDomain = domain.subList(estQuantilInf.get(1).intValue(), estQuantilSup.get(1).intValue());;

                    this.Vinf = new ArrayList<>();
                    this.Vsup = new ArrayList<>();
                    this.GKQuantileEstimator = new GreenwaldKhannaQuantileEstimator(eps);
                    return this.stableDomain;
                }
                // remove oldest tuple to remain size of stable domain
                this.Vinf.remove(0);
                this.Vsup.remove(0);
            }
        }
        return null;
    }

    /**
     * Calculates the standard deviation of a list of numbers
     *
     * @param numbers list of numbers from which the standard deviation should be calculated
     * @return standard deviation of the list of numbers
     */
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

        //Todo: Add update tau()
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

    protected double[][] doca(List<List<Double>> X, boolean inplace) {
        double[][] dataArr = X.stream()
                .map(row -> row.stream()
                        .mapToDouble(Double::doubleValue)
                        .toArray())
                .toArray(double[][]::new);

        // Create Cluster
        List<List<Integer>> clusters = this.onlineClustering(dataArr);

        double sensitivity = Math.abs((getMax(dataArr) - getMin(dataArr)));
        int numAttributes = X.get(0).size();
        int numInstances = X.size();

        // Create Output structure
        double[][] output;
        if (inplace) {
            output = dataArr;
        } else {
            output = new double[numInstances][numAttributes];
        }

        for (List<Integer> cs : clusters) {
            double[] mean = new double[numAttributes];
            for (int i : cs) {
                for (int j = 0; j < numAttributes; j++) {
                    mean[j] += dataArr[i][j];
                }
            }
            for (int j = 0; j < numAttributes; j++) {
                mean[j] /= cs.size();
            }
            double scale = (sensitivity / (cs.size() * eps));
            double[] laplace = new double[numAttributes];
            for (int j = 0; j < numAttributes; j++) {
                laplace[j] = Math.random() - 0.5;
            }
            for (int j = 0; j < numAttributes; j++) {
                output[cs.get(0)][j] = mean[j] + scale * laplace[j];
            }
        }

        return output;
    }

    /**
      * Online Clustering Algorithm
     *
      * @param dataArr Data Set to be clustered
      * @return
      */
    private List<List<Integer>> onlineClustering(double[][] dataArr) {
        int num_attributes = dataArr[0].length;
        int num_instances = dataArr.length;


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

        for (int clock = 0; clock < num_instances; clock++) {
            double[] dataPoint = dataArr[clock];

            // Update min/max
            for (int i = 0; i < num_attributes; i++) {
                mn[i] = Math.min(mn[i], dataPoint[i]);
                mx[i] = Math.max(mx[i], dataPoint[i]);
            }

            double[] dif = new double[num_attributes];
            for (int i = 0; i < num_attributes; i++) {
                dif[i] = mx[i] - mn[i];
            }

            // Find best cluster
            Integer best_cluster = this.findBestCluster(dataPoint, clusters, mn_c, mx_c, dif);

            // Add tuple to cluster
            if (best_cluster == -1) {
                // Add new Cluster
                List<Integer> newCluster = new ArrayList<>();
                newCluster.add(clock);
                clusters.add(newCluster);

                // Set Min/Max of new Cluster
                mn_c.add(dataPoint.clone());
                mx_c.add(dataPoint.clone());
            } else {
                clusters.get(best_cluster).add(clock);
                // Update min/max
                double[] mn_cluster = mn_c.get(best_cluster);
                double[] mx_cluster = mx_c.get(best_cluster);
                mn_cluster[best_cluster] = Math.min(mn_cluster[best_cluster], dataPoint[best_cluster]);
                mx_cluster[best_cluster] = Math.max(mx_cluster[best_cluster], dataPoint[best_cluster]);
            }

            List<Integer> overripe_clusters = new ArrayList<>();
            for (int c = 0; c < clusters.size(); c++) {
                if (clusters.get(c).contains(clock - delayConstraint)) {
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
                this.tau = losses.stream().mapToDouble(Double::doubleValue).sum() / losses.size();
                clusters_final.add(clusters.get(c));
                clusters.remove(c);
                mn_c.remove(c);
                mx_c.remove(c);
            }
        }

        clusters_final.addAll(clusters);
        System.out.println("Number of Clusters: " + clusters_final.size());
        System.out.println("Number of Instances: " + num_instances);
        return clusters_final;
    }

    /**
     * Find the best cluster for a given data point
     * @param dataPoint
     * @param clusters
     * @param mx_c
     * @param mn_c
     * @param dif
     * @return
     */
    private int findBestCluster(double[] dataPoint, List<List<Integer>> clusters, List<double[]> mx_c, List<double[]> mn_c, double[] dif) {
        int best_cluster = -1;
        int numAttributes = dataPoint.length;


        // Calculate enlargement (the value is not yet divided by the number of attributes!)
        double[] enlargement = new double[clusters.size()];
        for (int c = 0; c < clusters.size(); c++) {
            double sum = 0;
            for (int i = 0; i < numAttributes; i++) {
                sum += Math.max(0, dataPoint[i] - mx_c.get(c)[i]) - Math.min(0, dataPoint[i] - mn_c.get(c)[i]);
            }
            enlargement[c] = sum;
        }

        double min_enlarge;
        if (enlargement.length == 0) {
            min_enlarge = Double.POSITIVE_INFINITY;
        } else {
            min_enlarge = Arrays.stream(enlargement).min().getAsDouble();
        }

        List<Integer> ok_clusters = new ArrayList<>();
        List<Integer> min_clusters = new ArrayList<>();

        for (int c = 0; c < clusters.size(); c++) {
            double enl = enlargement[c];
            if (enl == min_enlarge) {
                min_clusters.add(c);
                double overall_loss = (enl + getSumOfElementsInArray(divisionWith0(mx_c.get(c), dif))) / numAttributes;
                if (overall_loss <= this.tau) {
                    ok_clusters.add(c);
                }
            }
        }

        if (!ok_clusters.isEmpty()) {
            best_cluster = ok_clusters.stream()
                            .min((c1, c2) -> Integer.compare(clusters.get(c1).size(), clusters.get(c2).size()))
                            .orElse(-1);
        } else if (clusters.size() >= beta) {
            best_cluster = min_clusters.stream()
                            .min((c1, c2) -> Integer.compare(clusters.get(c1).size(), clusters.get(c2).size()))
                            .orElse(-1);
        }
        return best_cluster;
    }

}
