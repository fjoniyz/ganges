package myapps;

public class Pair {
    private double[] firstElement;
    private double[] secondElement;

    public Pair(double[] firstElement, double[] secondElement){
        this.firstElement = firstElement;
        this.secondElement = secondElement;
    }

    public double getMax(double[] arr) {
        double max = 0;
        for(double d: arr) {
            if(d > max) {
                max = d;
            }
        }
        return max;
    }

    public double getMin(double[] arr) {
        double max = 0;
        for(double d: arr) {
            if(d < max) {
                max = d;
            }
        }
        return max;
    }

    public double getLargestElement() {
        double largestInFirstElement = getMax(firstElement);
        double largestInSecondElement = getMax(secondElement);
        return Math.max(largestInFirstElement, largestInSecondElement);
    }

    public double getSmallestElement() {
        double largestInFirstElement = getMin(firstElement);
        double largestInSecondElement = getMin(secondElement);
        return Math.max(largestInFirstElement, largestInSecondElement);
    }
}
