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
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        List<Double> weights = new ArrayList<Double>();
        weights.add(1.0);
        weights.add(2.0);

        String input = tuple.getString(1);
        List<Double> result = Util.parse_str_vector(input);

        collector.emit(new Values(tuple.getValue(0), result));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "result"));
    }
}
