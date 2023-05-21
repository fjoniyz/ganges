package myapps;

import java.io.*;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.IntStream;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;

public class Doca {

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

  public static Double[] div0(double[] a, double[] b) {
    Double[] result = new Double[a.length];
    for (int i = 0; i < a.length; i++) {
      result[i] = (b[i] != 0) ? a[i] / b[i] : 0;
    }
    return result;
  }

  public static double getSum(double[] values) {
    double sum = 0;
    for (double value : values) {
      sum += value;
    }
    return sum;
  }

  private static Double[][] extractColumns(List<Double[]> dataList, List<String> columns) {
    int numRows = dataList.size();
    int numCols = columns.size();
    Double[][] extractedData = new Double[numRows][numCols];
    for (int i = 0; i < numRows; i++) {
      Double[] row = dataList.get(i);
      for (int j = 0; j < numCols; j++) {
        extractedData[i][j] = row[columns.indexOf(columns.get(j))];
      }
    }
    return extractedData;
  }

  private static List<Double[]> readCSVFile(String filePath) {
    List<Double[]> dataList = new ArrayList<>();
    try (CSVParser csvParser = CSVFormat.DEFAULT.withHeader().parse(new FileReader(filePath))) {
      for (CSVRecord record : csvParser) {
        Double[] row = new Double[record.size()];
        for (int i = 0; i < record.size(); i++) {
          row[i] = Double.parseDouble(record.get(i));
        }
        dataList.add(row);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return dataList;
  }

  private static void mapIncomeColumn(Double[][] data) {
    int incomeColIndex = 6; // Assuming 'income' is the last column (index 6)
    for (int i = 0; i < data.length; i++) {
      if (data[i][incomeColIndex] == 0) {
        data[i][incomeColIndex] = 0.0;
      } else {
        data[i][incomeColIndex] = 1.0;
      }
    }
  }

  private static double[] extractColumns(Double[][] data, int index) {
    double[] res = new double[data.length];
    int k = 0;
    for (Double[] rows : data) {
      res[k] = rows[index];
      k++;
    }
    return res;
  }

  private static double[][] normalizeDataFrame(Double[][] data) {
    int numRows = data.length;
    int numCols = data[0].length;
    double[][] normalizedData = new double[numRows][numCols];
    double[] columnStdDevs = new double[numCols];

    for (int i = 0; i < numCols; i++) {
      double[] columnData = extractColumns(data, i);
      StandardDeviation std = new StandardDeviation();
      columnStdDevs[i] = std.evaluate(columnData);
    }
    for (int i = 0; i < numRows; i++) {
      for (int j = 0; j < numCols; j++) {
        normalizedData[i][j] = data[i][j] / columnStdDevs[j];
      }
    }
    return normalizedData;
  }

  private static void printResult(double[][] result) {
    int k = 0;
    for (double[] row : result) {
      System.out.println("Line: " + k);
      for (double value : row) {
        System.out.print("Value: " + value + " ");
      }
      k++;
      System.out.println();
    }
  }
  public static double sum(double[] input){
    double sum = 0;
    for(double d: input){
      sum += d;
    }
    return sum;
  }
  public static ArrayList<double[]> zip(double[] a, double[] b) {
    ArrayList<double[]> res = new ArrayList<>();
    int maxLength = Math.min(a.length, b.length);
    for(int i = 0; i < maxLength; i++){
      res.add(new double[] {a[i], b[i]});
    }
    return res;
  }

  public static double[] vectorize(
          BiFunction<Double, Double, Double> function, double[] a, double[] b) {
    Predicate<Integer> nonZero = value -> value != 0;
    return IntStream.range(0, Math.min(a.length, b.length))
            .mapToDouble(i -> function.apply(a[i], b[i]))
            .toArray();
  }

  public static double[][] doca(
      double[][] X, double eps, int delay_constraint, int beta, int mi, boolean inplace) {
    int num_instances = X.length;
    int num_attributes = X[0].length;
    BiFunction<Double, Double, Double> divisionFunction = (a, b) -> b != 0 ? (double) a / b : 0;

    double sensitivity = 1.5 * (getMax(X) - getMin(X));

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
        ArrayList<ArrayList<double[]>> zippedEntries = new ArrayList<>();
        for(int i = 0; i < mn_c.size(); i++){
          zippedEntries.set(i, zip(mn_c.get(i), mx_c.get(i)));
        }
        // Calculate enlargement (the value is not yet divided by the number of attributes!)
        ArrayList<Double[]> enlargement = new ArrayList<>();
        double[] zeros = new double[zippedEntries.size()];
        for (int c = 0; c < clusters.size(); c++) {
          double sum = 0;
          for (int i = 0; i < num_attributes; i++) {
            enlargement.set(i,
                div0(maximumArray(zeros, subtractArrays(data_point, zippedEntries.get(c).get(i))), dif));
          }
        }

        double min_enlarge = 30;

        List<Integer> ok_clusters = new ArrayList<>();
        List<Integer> min_clusters = new ArrayList<>();

        for (int c = 0; c < clusters.size(); c++) {
          double enl = enlargement.get(c);
          if (enl == min_enlarge) {
            min_clusters.add(c);
            double overall_loss = (enl + getSum(div0(mx_c.get(c), dif))) / num_attributes;
            if (overall_loss <= tau) {
              ok_clusters.add(c);
            }
          }
        }

        if (!ok_clusters.isEmpty()) {
          TODOREMOVE_Perfect += 1;
          //Finds the smallest item in ok_clusters
          best_cluster =
              ok_clusters.stream()
                  .min(
                          Comparator.comparingInt(c -> clusters.get(c).size()))
                  .orElse(null);
        } else if (clusters.size() >= beta) {
          //Find the smallest item in min_clusters
          best_cluster =
              min_clusters.stream()
                  .min(
                          Comparator.comparingInt(c -> clusters.get(c).size()))
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
        for (int i = 0; i < num_attributes; i++) {
          mn_cluster[best_cluster] = Math.min(mn_cluster[best_cluster], data_point[best_cluster]);
          mx_cluster[best_cluster] = Math.max(mx_cluster[best_cluster], data_point[best_cluster]);
        }
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
        double loss = getSum(div0(dif_cluster, dif)) / num_attributes;
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

  public static double[] subtractArrays(double[] first, double[] second) {
    int min_length = Math.min(first.length, second.length);
    double[] result = new double[min_length];
    for(int i = 0; i < min_length; i++){
      result[i] = first[i] - second[i];
    }
    return result;
  }

  public static double[] maximumArray(double[]a, double[]b) {
    int min_length = Math.min(a.length, b.length);
    int max_length = Math.max(a.length, b.length);
    int difference = max_length - min_length;
    double[] results_arr = new double[max_length];
    for(int i = 0; i < min_length; i++){
      results_arr[i] = Math.max(a[i], b[i]);
    }
    if(a.length > b.length){
      if (max_length - difference >= 0)
        System.arraycopy(a, difference, results_arr, difference, max_length - difference);
    }else{
      if (max_length - difference >= 0)
        System.arraycopy(b, difference, results_arr, difference, max_length - difference);
    }
    return results_arr;
  }

  public static double[] fill(double[] arrayToFill, int startIndex, int endIndex, double value){
    for(int i = startIndex; i < endIndex; i++){
      arrayToFill[i] = value;
    }
    return arrayToFill;
  }

  public static void main(String[] args) {
    List<Double[]> dataList = readCSVFile("./adult_train.csv");

    List<String> columns =
        Arrays.asList(
            "age",
            "education-num",
            "marital-status",
            "gender",
            "capital-gain",
            "hours-per-week",
            "income");
    Double[][] data = extractColumns(dataList, columns);

    mapIncomeColumn(data);

    double[][] normalizedData = normalizeDataFrame(data);

    double[][] res = doca(normalizedData, 100, 1000, 50, 100, false);

    printResult(res);
  }
}