package com.ganges.anoniks;

import com.ganges.anonlib.castleguard.CGConfig;
import com.ganges.anonlib.castleguard.CGItem;
import com.ganges.anonlib.castleguard.CastleGuard;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ConsoleApp {
    public static void main(String[] args) throws IOException {
        BufferedReader reader = new BufferedReader(
                new InputStreamReader(System.in));

        CGConfig config = new CGConfig(2, 2, 5, 1, 5, 1, 70, true);

        // Reading data using readLine
        List<String> headers = new ArrayList<>(List.of(new String[] {"sensitive", "attr1", "attr2"}));

        CastleGuard anonymization = new CastleGuard(config, headers, headers.get(0));
        while (true) {
            String text = reader.readLine();
            if (text.equals("exit")) {
                break;
            }

            if (!text.matches("([0-9]*,)*[0-9]*")) {
                System.out.println("incorrect format");
                continue;
            }
            List<Double> values = Stream.of(text.split(","))
                    .map(Double::parseDouble)
                    .collect(Collectors.toList());
            HashMap<String, Double> rowData = new HashMap<>();
            for (int i = 0; i < values.size(); i++) {
                rowData.put(headers.get(i), values.get(i));
            }
            anonymization.insertData(rowData);
            Optional<CGItem> anonData = anonymization.tryGetOutputLine();
            if (anonData.isPresent()) {
                System.out.print("Anon data: ");
                anonData.get().getData().forEach((key, value) -> {
                    if (key.startsWith("spc")) {
                        System.out.print(key.replace("spc", "") + ": " + value + " ");
                    }
                });
                System.out.println();
            }
        }

        // Printing the read line
    }
}