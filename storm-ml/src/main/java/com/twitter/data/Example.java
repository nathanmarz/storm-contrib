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

    public String toString() {
        return label + ":" + Arrays.toString(x);
    }
}
