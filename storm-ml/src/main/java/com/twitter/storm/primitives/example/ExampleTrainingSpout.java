package com.twitter.storm.primitives.example;

import backtype.storm.tuple.Values;

import com.twitter.storm.primitives.BaseTrainingSpout;

public class ExampleTrainingSpout extends BaseTrainingSpout {
    int samples_count = 0;
    int max_samples = 100;

    public static double get_label(Double x, Double y) {
        // arbitrary expected output (for testing purposes)
        return (2 * x + -1 < y) ? 1.0 : -1.0;
    }

    public void nextTuple() {
        if (this.samples_count < this.max_samples) {
            Double x = 100 * Math.random();
            Double y = 5.0;
            double label = ExampleTrainingSpout.get_label(x, y);

            _collector.emit(new Values(x, y, label));

            this.samples_count++;
        }
    }
}
