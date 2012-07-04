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

public class EvaluationBolt extends BaseBasicBolt {
    Double bias;
    Double threshold;

    public EvaluationBolt(Double bias, Double threshold) {
        this.bias = bias;
        this.threshold = threshold;
    }

    List<Double> get_latest_weigths() {
        List<Double> weights = new ArrayList<Double>();
        weights.add(1.0);
        weights.add(2.0);

        return weights;
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        List<Double> weights = get_latest_weigths();

        String input_str = tuple.getString(1);
        List<Double> input = Util.parse_str_vector(input_str);

        Double result = Util.dot_product(input, weights) + this.bias;

        collector.emit(new Values(tuple.getValue(0), result > this.threshold ? 1 : 0));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "result"));
    }
}
