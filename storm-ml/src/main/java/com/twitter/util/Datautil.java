package com.twitter.util;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Datautil {

    public static List<Double> parse_str_vector(String str_vector) {
        List<Double> vector = new ArrayList<Double>();

        Scanner scanner = new Scanner(str_vector.substring(1, str_vector.length() - 1));
        scanner.useDelimiter(", ");

        while (scanner.hasNextDouble())
            vector.add(scanner.nextDouble());

        return vector;
    }

    public static Double dot_product(List<Double> vector_a, List<Double> vector_b) {
        Double result = 0.0;

        for (int i = 0; i < vector_a.size(); i++)
            result += vector_a.get(i) * vector_b.get(i);

        return result;
    }

    public List<Double[]> readTrainingFile() {
        List<Double[]> lines = new ArrayList<Double[]>();
        String strLine;
        try {
            BufferedReader br = new BufferedReader(new FileReader("src/main/resources/testSet.txt"));
            while ((strLine = br.readLine()) != null) {
                String[] values = strLine.split("\\t");
                Double[] line = new Double[3];
                for (int i = 0; i <= 2; i++) {
                    line[i] = Double.parseDouble(values[i]);
                }
                lines.add(line);
            }

        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return lines;
    }

    public static String toStrVector(List<Double> aggregateWeights) {
        String acc = "[";
        for (Double weight : aggregateWeights) {
            acc += weight.toString() + ", ";
        }
        acc += "]";
        return acc;
    }
}
