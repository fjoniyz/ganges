package com.ganges.lib.castleguard;

import java.io.*;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import myapps.Doca;

public class Prognose {

    public static void main(String[] args){
        createDocaPrognoseData();
        //reateCastleGuardPrognoseData();
    }
/*
    private static void createCastleGuardPrognoseData() {
        String csvFile = "emobilitydata10000.csv";
        String resFile = "castleguard_anon_10000.csv";

        List<HashMap<String, Float>> data = loadCSV(csvFile);

        ArrayList<String> headers = new ArrayList<>();
        headers.add("start_time_loading");
        headers.add("end_time_loading");
        headers.add("kwh");
        headers.add("loading_potential");

        CGConfig config = new CGConfig(10, 80, 20, 1, 23, 1, 10 * Math.log(2), true);
        CastleGuard castleGuard = new CastleGuard(config, headers, "start_loading_time");

        List<HashMap<String, Float>> anonymizedData = new ArrayList<>();

        for (HashMap<String, Float> rowData : data) {
            castleGuard.insertData(rowData);
            Optional<HashMap<String, Float>> anonData = castleGuard.tryGetOutputLine();
            if (anonData.isPresent()) {
                anonymizedData.add(anonData.get());
                System.out.println(anonData);
            }
        }
        List<HashMap<String, Float>> processedData = calculateMean(anonymizedData);
        writeCSV(processedData, resFile);
    }
*/
    private static void createDocaPrognoseData() {
        Doca doca = new Doca();

        String csvFile = "emobilitydata10000.csv";
        String resFile = "doca_anon_10000-no_domain_bounding.csv";

        List<Map<String, Double>> data = loadCSVDouble(csvFile);
        List<Map<String, Double>> result = doca.anonymize(data);
        writeCSVDouble(result, resFile);
    }
    /*
    public static List<HashMap<String, Float>> calculateMean(List<HashMap<String, Float>> entries) {
        List<HashMap<String, Float>> processedEntries = new ArrayList<>();

        for (HashMap<String, Float> entry : entries) {
            Float startLoadingTime = (entry.get("minstart_time_loading") + entry.get("maxstart_time_loading")) / 2;
            Float endLoadingTime = (entry.get("minend_time_loading") + entry.get("maxend_time_loading")) / 2;
            Float kwh = (entry.get("minkwh") + entry.get("maxkwh")) / 2;
            Float loadingPotential = (entry.get("minloading_potential") + entry.get("maxloading_potential")) / 2;
            processedEntries.add(new HashMap<>(){{
                put("start_time_loading", startLoadingTime);
                put("end_time_loading", endLoadingTime);
                put("kwh", kwh);
                put("loading_potential", loadingPotential);}});
        }

        return processedEntries;
    }
*/

    public static void writeCSV(List<HashMap<String, Float>> data, String csvFile) {
        try (FileWriter writer = new FileWriter(csvFile)) {
            if (!data.isEmpty()) {
                // Write headers
                HashMap<String, Float> firstRow = data.get(0);
                for (String header : firstRow.keySet()) {
                    writer.append(header).append(",");
                }
                writer.append("\n");

                // Write data
                for (HashMap<String, Float> row : data) {
                    for (Float value : row.values()) {
                        writer.append(value.toString()).append(",");
                    }
                    writer.append("\n");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void writeCSVDouble(List<Map<String, Double>> data, String csvFile) {
        try (FileWriter writer = new FileWriter(csvFile)) {
            if (!data.isEmpty()) {
                // Write headers
                Map<String, Double> firstRow = data.get(0);
                for (String header : firstRow.keySet()) {
                    writer.append(header).append(",");
                }
                writer.append("\n");

                // Write data
                for (Map<String, Double> row : data) {
                    for (Double value : row.values()) {
                        writer.append(value.toString()).append(",");
                    }
                    writer.append("\n");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static List<HashMap<String, Float>> loadCSV(String csvFile) {
        List<HashMap<String, Float>> data = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
            String line;
            String[] headers = null;
            int headerIndex;

            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");

                if (headers == null) {
                    // First line contains the headers
                    headers = values;
                    continue;
                }

                HashMap<String, Float> row = new HashMap<>();

                for (String header : headers) {
                    headerIndex = getIndex(header);


                    if (headerIndex != -1) {
                        try {
                            Float value = Float.parseFloat(values[headerIndex]);
                            row.put(header, value);
                        } catch (NumberFormatException e) {
                            System.out.println("Unable to parse value for header: " + header);
                        }
                    }

                }

                data.add(row);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return data;
    }

    public static List<Map<String, Double>> loadCSVDouble(String csvFile) {
        List<Map<String, Double>> data = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
            String line;
            String[] headers = null;
            int headerIndex;

            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");

                if (headers == null) {
                    // First line contains the headers
                    headers = values;
                    continue;
                }

                Map<String, Double> row = new HashMap<>();

                for (String header : headers) {
                    headerIndex = getIndex(header);

                    if (headerIndex != -1) {
                        try {
                            double value = Double.parseDouble(values[headerIndex]);
                            row.put(header, value);
                        } catch (NumberFormatException e) {
                            System.out.println("Unable to parse value for header: " + header);
                        }
                    }
                }

                data.add(row);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return data;
    }

    public static int getIndex(String header) {
        String[] desiredHeaders = {"start_time_loading", "end_time_loading", "kwh", "loading_potential"};

        for (int i = 0; i < desiredHeaders.length; i++) {
            if (desiredHeaders[i].equals(header)) {
                return i;
            }
        }

        return -1;
    }
}




