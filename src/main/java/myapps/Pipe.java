package myapps;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;

/**
 * In this example, we implement a simple Pipe program using the high-level Streams DSL that reads
 * from a source topic "streams-plaintext-input", where the values of messages represent lines of
 * text, and writes the messages as-is into a sink topic "streams-pipe-output".
 */
public class Pipe {

  public static double[][] deleteLastRow(double[][] array) {
    if (array == null || array.length == 0) {
        throw new IllegalArgumentException("Array cannot be null or empty.");
    }

    int numRows = array.length - 1;
    double[][] newArray = new double[numRows][];

    // Copy all rows except the last one
    for (int i = 0; i < numRows; i++) {
        newArray[i] = array[i];
    }

    return newArray;
}
  public static void main(final String[] args) throws CsvException, FileNotFoundException, IOException {

      String filename = "adult_test.csv";
      String filePath = System.getProperty("user.dir") + File.separator + filename;
      try(CSVReader reader = new CSVReader(new FileReader(filePath))) {
        List<String[]> rows = reader.readAll();
        double[][] dataArray = new double[rows.size()][];
        int k = 0;
        for(int i = 0; i < rows.size(); i++){
          if(i != 0) {
            dataArray[k] =  Arrays.stream(rows.get(i))
            .mapToDouble(Double::parseDouble)
            .toArray();
            k += 1;
          }
        }
        double[][] deletedLastRow = deleteLastRow(dataArray);
        double[][]output = Doca.doca(deletedLastRow, 1000000, 1000, 50, 100, false);
        for(double[] dArray: output){
          for(double d: dArray){
            System.out.print(d + ",");
          }
          System.out.println();
        }
      }
    }
  }
