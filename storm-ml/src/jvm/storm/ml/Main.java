package storm.ml;

import java.lang.Boolean;
import java.math.BigDecimal;
import java.util.List;
import java.util.ArrayList;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import java.util.Map;

import org.javatuples.Pair;

import storm.ml.PerceptronTopologyBuilder;

public class Main {
    
    public static class TrainingSpout extends BaseRichSpout {
        OutputCollector _collector;

        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            _collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
            _t
            _collector.emit(tuple, new Values(tuple.getString(0) + "!!!"));
            _collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }

    }

    public static class TrainingBolt extends BaseRichBolt {
        OutputCollector _collector;

        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            _collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
            _t
            _collector.emit(tuple, new Values(tuple.getString(0) + "!!!"));
            _collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }

    }

    public perceptronTopology() {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("word", new TestWordSpout(), 10);
        builder.setBolt("exclaim1", new ExclamationBolt(), 3).shuffleGrouping("word");
        builder.setBolt("exclaim2", new ExclamationBolt(), 2).shuffleGrouping("exclaim1");

        Config conf = new Config();
        conf.setDebug(true);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);

            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());
            Utils.sleep(10000);
            cluster.killTopology("test");
            cluster.shutdown();
        }
    }

    public static void main(String[] keywords) {
        perceptronTopology();
//        List<Pair<List<BigDecimal>, Boolean>> training_set = new ArrayList<Pair<List<BigDecimal>, Boolean>>(4);
//        List<BigDecimal> input_vector = new ArrayList<BigDecimal>(3);
//
//        input_vector.add(new BigDecimal(1));
//        input_vector.add(new BigDecimal(0));
//        input_vector.add(new BigDecimal(0));
//        training_set.add(new Pair<List<BigDecimal>, Boolean>(input_vector, true));
//
//        input_vector.add(new BigDecimal(1));
//        input_vector.add(new BigDecimal(0));
//        input_vector.add(new BigDecimal(1));
//        training_set.add(new Pair<List<BigDecimal>, Boolean>(input_vector, true));
//
//        input_vector.add(new BigDecimal(1));
//        input_vector.add(new BigDecimal(1));
//        input_vector.add(new BigDecimal(0));
//        training_set.add(new Pair<List<BigDecimal>, Boolean>(input_vector, true));
//
//        input_vector.add(new BigDecimal(1));
//        input_vector.add(new BigDecimal(1));
//        input_vector.add(new BigDecimal(1));
//        training_set.add(new Pair<List<BigDecimal>, Boolean>(input_vector, false));
//
//        PerceptronTopologyBuilder ptb = new PerceptronTopologyBuilder(3, 0.5, 0.1);
//        ptb.train(training_set);
    }
}
