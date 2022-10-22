package com.emse.worker.helper;

import org.javatuples.Pair;
import org.javatuples.Triplet;

import java.io.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;

public class Processing {
    public static HashMap<String, Triplet<Integer, Double, Double>> read(BufferedReader reader) {
        HashMap<String, Triplet<Integer, Double, Double>> productData = new HashMap<>(); //Triplet(count, unit_price, unit_cost)
        reader.lines().skip(1).forEach(line -> {
            String[] data = line.split(";");
            Triplet<Integer, Double, Double> values;
            try {
                values = new Triplet<>(Integer.parseInt(data[3]), Double.parseDouble(data[4]), Double.parseDouble(data[6]));
            } catch (NumberFormatException e) {
                values = new Triplet<>(0,0.0,0.0);
            }
            addValue(productData, data[2], values);
        });

        return productData;
    }

    public static String computeAndWrite(HashMap<String, Triplet<Integer, Double, Double>> dataMap, String store) {
        //computing all necessary values
        Double total = 0.0;
        for (String key : dataMap.keySet()) {
            Triplet<Integer, Double, Double> data = dataMap.get(key);
            dataMap.put(key, data.setAt1(data.getValue0()*data.getValue1()).setAt2(data.getValue0()*data.getValue2()));
            total+=dataMap.get(key).getValue2();
        }

        //writing
        StringWriter stringWriter;
        try {
            stringWriter = new StringWriter();
            BufferedWriter bufferedWriter = new BufferedWriter(stringWriter);
            bufferedWriter.write(store + ";;;" + round(total));
            for (String key : dataMap.keySet()) {
                bufferedWriter.newLine();
                bufferedWriter.write(key + ";"
                        + dataMap.get(key).getValue0() + ";"
                        + round(dataMap.get(key).getValue1()) + ";"
                        + round(dataMap.get(key).getValue2()));
            }
            bufferedWriter.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return String.valueOf(stringWriter.getBuffer());
    }

    public static Pair<String, String> getFileData(String fileName) { //returns (newFileName, store)
        String[] parts = fileName.split("-");
        String newFileName = parts[0] + "-" + parts[1] + "-" + parts[2] + "_" + parts[3];
        String store = parts[3].split("\\.")[0];
        return new Pair<>(newFileName, store);
    }

    private static void addValue(HashMap<String, Triplet<Integer, Double, Double>> map, String key, Triplet<Integer, Double, Double> values) {
        map.merge(key, values, (a, b) -> a.setAt0(a.getValue0() + b.getValue0()));
    }

    private static double round(double value) {
        BigDecimal bd = BigDecimal.valueOf(value);
        bd = bd.setScale(2, RoundingMode.HALF_UP);
        return bd.doubleValue();
    }
}
