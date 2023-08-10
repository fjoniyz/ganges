package com.ganges.lib.doca;

import com.ganges.lib.AnonymizationAlgorithm;
import com.ganges.lib.AnonymizationItem;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class Doca implements AnonymizationAlgorithm {

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

    public static String[] getParameters() {
        String[] result = new String[4];
        String userDirectory = System.getProperty("user.dir");
        try(InputStream inputStream = Files.newInputStream(Paths.get(userDirectory+"/src/main/resources/doca.properties"))){
            Properties properties = new Properties();
            properties.load(inputStream);
            result[0] = properties.getProperty("eps");
            result[1] = properties.getProperty("delay_constraint");
            result[2] = properties.getProperty("beta");
            result[3] = properties.getProperty("inplace");
            return result;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<AnonymizationItem> anonymize(List<AnonymizationItem> X) {
        if (X.size() == 0 || X.get(0).getValues().size() == 0) {
            return new ArrayList<>();
        }

        // Preserving value order through anonymization input/output
        ArrayList<String> headers = new ArrayList<>(X.get(0).getValues().keySet());

        // Convert map to double array
        double[][] docaInput = new double[X.size()][];
        for (int i = 0; i < X.size(); i++) {
            docaInput[i] = new double[X.get(i).getValues().size()];
            for (int headerId = 0; headerId < headers.size(); headerId++) {
                docaInput[i][headerId] = X.get(i).getValues().get(headers.get(headerId));
            }
        }

        double[][] result = anonymize(docaInput);

        // Convert double array to map
        List<AnonymizationItem> outputResult = new ArrayList<>();
        for (int i = 0; i < result.length; i++) {
            Map<String, Double> dataRowMap = new LinkedHashMap<>();
            for (int headerId = 0; headerId < headers.size(); headerId++) {
                dataRowMap.put(headers.get(headerId), result[i][headerId]);
            }
            AnonymizationItem item = new AnonymizationItem(X.get(i).getId(), dataRowMap,
                new HashMap<>());
            outputResult.add(item);
        }
        if (outputResult.isEmpty()) {
            return outputResult;
        } else {
            return List.of(outputResult.get(outputResult.size()-1));
        }
    }


    public double[][] anonymize(
            double[][] x) {
        String[] parameters = getParameters();
        double eps = Double.parseDouble(parameters[0]);
        int delay_constraint = Integer.parseInt(parameters[1]);
        int beta = Integer.parseInt(parameters[2]);
        boolean inplace = Boolean.parseBoolean(parameters[3]);
        int num_instances = x.length;
        int num_attributes = x[0].length;

        double sensitivity = Math.abs((getMax(x) - getMin(x)));

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
            output = x;
        } else {
            output = new double[num_instances][num_attributes];
        }

        int TODOREMOVE_Perfect = 0;

        for (int clock = 0; clock < num_instances; clock++) {
            if (clock % 1000 == 0) {
                System.out.println("Clock " + clock + " " + TODOREMOVE_Perfect);
            }

            double[] data_point = x[clock];

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
                    mean[j] += x[i][j];
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