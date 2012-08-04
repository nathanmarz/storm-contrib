package com.twitter.algorithms;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import net.spy.memcached.MemcachedClient;

import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Logger;

import com.twitter.data.Example;
import com.twitter.storm.primitives.LocalLearner;
import com.twitter.util.Datautil;
import com.twitter.util.MathUtil;

public class Learner implements Serializable {
    public static Logger LOG = Logger.getLogger(LocalLearner.class);

    protected double[] weights;
    protected LossFunction lossFunction;
    int numExamples = 0;
    int numMisclassified = 0;
    double totalLoss = 0.0;
    double gradientSum = 0.0;
    protected double learningRate = 0.0;

    public Learner(int dimension) {
        weights = new double[dimension];
        lossFunction = new LossFunction(2);
    }

    public void setLocalWeights(List<Double> localWeights) {
        Double[] weights_double = localWeights.toArray(new Double[localWeights.size()]);
        this.setWeights(ArrayUtils.toPrimitive(weights_double));

    }

    public void update(Example example, int epoch, MemcachedClient memcache) {
        String cas_weights = (String) memcache.get("model");
        List<Double> weights = Datautil.parse_str_vector(cas_weights);
        setLocalWeights(weights);
        int predicted = predict(example);
        updateStats(example, predicted);
        LOG.debug("EXAMPLE " + example.x[0] + "," + example.x[1] + " LABEL" + example.label + " PREDICTED: "
                + predicted);
        if (example.isLabeled) {
            if ((double) predicted != example.label) {
                List<Double> gradient = lossFunction.gradient(example, predicted);
                gradientSum += MathUtil.l2norm(gradient);
                double eta = getLearningRate(example, epoch);
                LOG.debug("NEW WEIGHTS" + MathUtil.plus(weights, MathUtil.times(gradient, -1.0 * eta)));
                setLocalWeights(MathUtil.plus(weights, MathUtil.times(gradient, -1.0 * eta)));
            }
        }
        displayStats();
    }

    protected double getLearningRate(Example example, int timestamp) {
        return learningRate / Math.sqrt(timestamp);
    }

    public double[] getWeights() {
        return weights;
    }

    public List<Object> getWeightsArray() {
        List<Object> weight_array = new ArrayList<Object>();
        for (double weight : weights)
            weight_array.add(weight);
        return weight_array;
    }

    public double getParallelUpdateWeight() {
        return gradientSum;
    }

    public void initWeights(double[] newWeights) {
        assert (newWeights.length == weights.length);
        weights = Arrays.copyOf(newWeights, newWeights.length);
    }

    public int predict(Example example) {
        double dot = MathUtil.dot(weights, example.x);
        return (dot >= 0.0) ? 1 : -1;
    }

    protected void updateStats(Example example, int prediction) {
        numExamples++;
        if (example.label != prediction)
            numMisclassified++;
        totalLoss += lossFunction.get(example, prediction);
    }

    public void setWeights(double[] weights) {
        this.weights = weights;
    }

    public void displayStats() {
        if (numExamples == 0) {
            System.out.println("No examples seen so far.");
        }
        double accuracy = 1.0 - numMisclassified * 1.0 / numExamples;
        double meanLoss = totalLoss * 1.0 / numExamples;
        LOG.info(String.format("Accuracy: %g\tMean Loss: %g", accuracy, meanLoss));

    }

    public void resetStats() {
        numExamples = 0;
        numMisclassified = 0;
        totalLoss = 0.0;
    }
}
