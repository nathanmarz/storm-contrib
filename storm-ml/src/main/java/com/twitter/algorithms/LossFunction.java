package com.twitter.algorithms;

import java.io.Serializable;

import com.twitter.data.Example;

public class LossFunction implements Serializable {
    private double[] grad; // gradient

    public LossFunction(int dimension) {
        grad = new double[dimension];
    }

    public double get(Example e, int prediction) {
        return 0.5 * (e.label - prediction) * (e.label - prediction);
    }

    public double[] gradient(Example e, int prediction) {
        double f = -1.0 * (e.label - prediction);
        for (int i = 0; i < e.x.length; i++) {
            grad[i] = f * e.x[i];
        }
        return grad;
    }

    static LossFunction byName(String name, int dimension) {
        return new LossFunction(dimension);
    }

}
