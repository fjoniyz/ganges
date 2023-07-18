package com.ganges.lib.castleguard;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvValidationException;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Optional;
import java.util.Scanner;

public class Prognose {

    public static void main(String[] args){
        String currentPath = Paths.get("").toAbsolutePath().toString();

        ArrayList<String> headers = new ArrayList<>();
        headers.add("start_loading_time");
        headers.add("end_loading_time");
        headers.add("kwh");
        headers.add("loading_potential");

        CGConfig config = new CGConfig(3, 10, 5, 1, 5, 1, 100 * Math.log(2), true);
        CastleGuard castleGuard = new CastleGuard(config, headers, null);

        HashMap<Integer, HashMap<String, Float>> anonymizedData = new HashMap<>();

        try (CSVReader reader = new CSVReader(new FileReader("/Users/ingastrelnikova/Master/SoSe2023/ADSP/ganges/src/main/java/com/ganges/data/emobilitydata.csv"))) {
            String[] csvHeaders = reader.readNext(); // Read the header row

            String[] row;
            while ((row = reader.readNext()) != null) {
                HashMap<String, Float> rowData = new HashMap<>();
                for (int i = 0; i < csvHeaders.length; i++) {
                    String header = csvHeaders[i];
                    float value = Float.parseFloat(row[i]);
                    rowData.put(header, value);
                    castleGuard.insertData(rowData);
                    Optional<HashMap<String, Float>> anonData = castleGuard.tryGetOutputLine();
                    if (anonData.isPresent()) {
                        anonymizedData.put(i,anonData.get());
                        System.out.println(anonData);
                    }
                }

            }
        } catch (IOException | CsvValidationException e) {
            e.printStackTrace();
        }

//        try (CSVWriter writer = new CSVWriter(new FileWriter("results.csv"))) {
//            // Get the headers from the first row of data
//            HashMap<String, Float> firstRowData = anonymizedData.get(1);
//            String[] csvHeaders = firstRowData.keySet().toArray(new String[0]);
//            writer.writeNext(csvHeaders);
//
//            // Write rows
//            for (Integer key : anonymizedData.keySet()) {
//                HashMap<String, Float> rowData = anonymizedData.get(key);
//                String[] row = new String[csvHeaders.length];
//                for (int i = 0; i < csvHeaders.length; i++) {
//                    Float value = rowData.get(csvHeaders[i]);
//                    row[i] = (value != null) ? String.valueOf(value) : ""; // Handle null values
//                }
//                writer.writeNext(row);
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

    }
}




