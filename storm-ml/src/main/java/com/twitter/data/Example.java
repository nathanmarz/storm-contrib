package com.twitter.data;

import java.util.Arrays;

/**
 * @author Delip Rao
 */
public class Example {
    public double[] x;
    public double label;
    public boolean isLabeled;
    public double importance;
    public String tag;

    public Example(int dimension) {
        x = new double[dimension];
        isLabeled = false;
    }

    /**
     * 
     * @param example
     *            string representation of an example [+1,-1] | tag | importance | extra_info | feature:value pairs
     */
    public void parseFrom(String example, HashFunction hashFunction) {
        int dimension = x.length;
        example = example.trim();
        String[] toks = example.split("\\|");
        for (int i = 0; i < toks.length; i++) {
            toks[i] = toks[i].trim();
        }
        try {
            if (toks[0].equals("-1") || toks[0].equals("+1") || toks[0].equals("1") || toks[0].equals("0")) {
                // label = Integer.parseInt(toks[0]);
                isLabeled = true;
            }
            tag = toks[1];
            importance = 1.0;
            if (!toks[2].isEmpty()) {
                importance = Double.parseDouble(toks[2]);
            }
            String extraInfo = toks[3];
            // TODO (Delip): parse extraInfo
            for (String fv : toks[4].split("\\s+")) {
                String[] tmp = fv.split(":");
                String feature = tmp[0];
                double value = 1.0;
                if (tmp.length == 2) {
                    value = Double.parseDouble(tmp[1]);
                }
                int index = hashFunction.hash(feature, 0) % dimension;
                x[index] += value;
            }
        } catch (Throwable e) {
            System.err.println("Error Parsing:\n" + example);
            e.printStackTrace();
            return;
        }
    }

    public String toString() {
        return label + ":" + Arrays.toString(x);
    }
}
