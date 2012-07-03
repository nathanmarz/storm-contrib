package com.twitter.util;

import java.math.BigInteger;
import java.util.Arrays;

/**
 * Misc. math util functions
 * (refactor with Twitter specific ones)
 * @author Delip Rao
 */
public class MathUtil {
    public static double l2norm(double [] v) {
        double sum = 0;
        for (double d : v) {
            sum += d*d;
        }
        return sum;
    }

    public static double [] zero(double [] v) {
        for (int i = 0; i < v.length; i++) {
            v[i] = 0;
        }
        return v;
    }

    public static double [] times(double [] v, double factor) {
        for (int i = 0; i < v.length; i++) {
            v[i] *= factor;
        }
        return v;
    }

    public static double [] timesC(double [] v, double factor) {
        double [] vc = Arrays.copyOf(v, v.length);
        for (int i = 0; i < v.length; i++) {
            vc[i] *= factor;
        }
        return vc;
    }

    public static double [] plus(double [] v, double [] u) {
        for (int i = 0; i < v.length; i++) {
            v[i] += u[i];
        }
        return v;
    }

    public static double [] minus(double [] v, double [] u) {
        for (int i = 0; i < v.length; i++) {
            v[i] -= u[i];
        }
        return v;
    }

    public static double [] minusC(double [] v, double [] u) {
        double [] vc = Arrays.copyOf(v, v.length);
        for (int i = 0; i < v.length; i++) {
            vc[i] -= u[i];
        }
        return vc;
    }

    public static double dot(double [] u, double [] v) {
        double result = 0;
        for (int i = 0; i < v.length; i++) {
            result += u[i]*v[i];
        }
        return result;
    }

    public static int nextLikelyPrime(int n) {
        String s = String.valueOf(n - 1);
        return new BigInteger(s).nextProbablePrime().intValue();
    }
}
