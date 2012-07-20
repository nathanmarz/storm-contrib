package com.twitter.algorithms;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.twitter.data.Example;

public class LossFunction implements Serializable {
    private List<Double> grad; // gradient

    public LossFunction(int dimension) {
        grad = new ArrayList<Double>();
    }

    public double get(Example e, int prediction) {
        return 0.5 * (e.label - prediction) * (e.label - prediction);
    }

    public List<Double> gradient(Example e, int prediction) {
        double f = -1.0 * (e.label - prediction);
        for (int i = 0; i < e.x.length; i++) {
            grad.set(i, f * e.x[i]);
        }
        return grad;
    }

    static LossFunction byName(String name, int dimension) {
        return new LossFunction(dimension);
    }

}
