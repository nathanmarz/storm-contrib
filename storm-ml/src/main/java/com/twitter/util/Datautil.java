package com.twitter.util;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Datautil {

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
}
