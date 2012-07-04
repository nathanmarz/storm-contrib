package storm.ml.bolt;

import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.ArrayList;
import java.util.Scanner;
import java.util.List;

import storm.ml.Util;

public class TrainingBolt extends BaseBasicBolt {
    Double bias;
    Double threshold;
    Double learning_rate;
    BasicOutputCollector _collector;

    public TrainingBolt(Double bias, Double threshold, Double learning_rate) {
        this.bias          = bias;
        this.threshold     = threshold;
        this.learning_rate = learning_rate;
    }

    List<Double> get_latest_weights() {
        List<Double> weights = new ArrayList<Double>();
        weights.add(1.0);
        weights.add(2.0);

        return weights;
    }

    void cas_latest_weights(List<Double> old_weights, List<Double> new_weights) {
        return;
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        List<Double> weights = this.get_latest_weights();

        List<Double> example = Util.parse_str_vector(tuple.getString(0));
        int label            = tuple.getInteger(1);

        Double result = Util.dot_product(example, weights) + this.bias;
        int classif = result > this.threshold ? 1 : 0;

        int error = label - classif;
        if (error != 0) {
            List<Double> old_weights = new ArrayList<Double>(weights);

            for (int i=0; i<example.size(); i++) {
                weights.set(i, weights.get(i) + this.learning_rate * error * example.get(i));
            }
            this.cas_latest_weights(old_weights, weights);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) { }
}
