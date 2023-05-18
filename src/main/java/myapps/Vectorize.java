package myapps;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.IntStream;

public class Vectorize {
  public static void main(String[] args) {
    BiFunction<Double, Double, Double> divisionFunction = (a, b) -> b != 0 ? (double) a / b : 0;
    ArrayList<double[]> a = new ArrayList<>();
    a.add(new double[]{1,2,3});
    ArrayList<double[]> b = new ArrayList<>();
    b.add(new double[]{4, 5, 6});
//    System.out.println(getMaximumOfArray(a));
//    System.out.println(getMinimumOfArray(a));
    ArrayList<double[]> e = zip(a,b);
    for(double[] i: e){
      System.out.println("MNC: " + i[0] + " MXC: " + i[1]);
      System.out.println(Arrays.toString(i));
      double max = Math.max(i[0], i[1]);
      double min = Math.min(i[0], i[1]);
      System.out.println("Max: " + max);
      System.out.println("Min: " + min);
      System.out.println(Math.min(0, 10-i[0]) - Math.max(0, 10-i[1]));
      double[] result = vectorize(divisionFunction, new double[]{Math.min(0, 10 - i[0]) - Math.max(0, 10 - i[1]), Math.min(0, 10 - i[0]) - Math.max(0, 10 - i[1]), Math.min(0, 10 - i[0]) - Math.max(0, 10 - i[1])}, new double[]{3.97147428, 5.06335739, 2.00399096})
;
      System.out.println(sum(result));
    }
  }

  public static double sum(double[] input){
    double sum = 0;
    for(double d: input){
      sum += d;
    }
    return sum;
  }
  public static ArrayList<double[]> zip(ArrayList<double[]> a, ArrayList<double[]> b) {
    ArrayList<double[]> res = new ArrayList<>();
    int maxLength = Math.min(a.size(), b.size());
    for(int i = 0; i < maxLength; i++){
      res.add(new double[] {a.get(i)[i], b.get(i)[i]});
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
}
