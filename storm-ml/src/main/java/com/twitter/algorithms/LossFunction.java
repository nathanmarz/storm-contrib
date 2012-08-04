package com.twitter.algorithms;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.twitter.data.Example;
import com.twitter.storm.primitives.example.LocalLearner;

public class LossFunction implements Serializable {
    public static Logger LOG = Logger.getLogger(LocalLearner.class);

    public LossFunction(int dimension) {
    }

    public double get(Example e, int prediction) {
        return 0.5 * (e.label - prediction) * (e.label - prediction);
    }

    public List<Double> gradient(Example e, int prediction) {
        List<Double> grad = new ArrayList<Double>();
        double f = -1.0 * (e.label - prediction);
        for (int i = 0; i < e.x.length; i++) {
            grad.add(f * e.x[0]);
        }
        return grad;
    }

    static LossFunction byName(String name, int dimension) {
        return new LossFunction(dimension);
    }

}
