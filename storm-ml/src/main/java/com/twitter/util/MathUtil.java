package com.twitter.util;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MathUtil {
    public static double l2norm(List<Double> gradient) {
        double sum = 0;
        for (double d : gradient) {
            sum += d * d;
        }
        return sum;
    }

    public static double[] zero(double[] v) {
        for (int i = 0; i < v.length; i++) {
            v[i] = 0;
        }
        return v;
    }

    public static List<Double> times(List<Double> weights, double factor) {
        for (Double weight : weights) {
            weight *= factor;
        }
        return weights;
    }

    public static double[] timesC(double[] v, double factor) {
        double[] vc = Arrays.copyOf(v, v.length);
        for (int i = 0; i < v.length; i++) {
            vc[i] *= factor;
        }
        return vc;
    }

    public static List<Double> plus(List<Double> weights, double[] u) {
        for (int i = 0; i < u.length; i++) {
            Double weight = weights.get(i);
            weight += u[i];
            weights.set(i, weight);
        }
        return weights;
    }

    public static double[] minus(double[] v, double[] u) {
        for (int i = 0; i < v.length; i++) {
            v[i] -= u[i];
        }
        return v;
    }

    public static double[] minusC(double[] v, double[] u) {
        double[] vc = Arrays.copyOf(v, v.length);
        for (int i = 0; i < v.length; i++) {
            vc[i] -= u[i];
        }
        return vc;
    }

    public static double dot(double[] u, double[] v) {
        double result = 0;
        for (int i = 0; i < v.length; i++) {
            result += u[i] * v[i];
        }
        return result;
    }

    public static int nextLikelyPrime(int n) {
        String s = String.valueOf(n - 1);
        return new BigInteger(s).nextProbablePrime().intValue();
    }

    public static List<Double> plus(List<Double> aggregateWeights, List<Double> weight) {
        List<Double> result = new ArrayList<Double>();
        for (int i = 0; i < aggregateWeights.size(); i++) {
            result.add(aggregateWeights.get(i) * weight.get(i));
        }
        return result;
    }
}
