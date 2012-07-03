package com.twitter.algorithms;

import java.util.Arrays;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.transactional.ICommitter;
import backtype.storm.tuple.Tuple;

import com.twitter.util.MathUtil;

public class Aggregator extends BaseRichBolt implements ICommitter {

    public static Logger LOG = Logger.getLogger(Aggregator.class);
    double[] aggregateWeights = null;
    double totalUpdateWeight = 0.0;

    public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, Object id) {
        // TODO Auto-generated method stub

    }

    public void execute(Tuple tuple) {

        double[] weight = (double[]) tuple.getValue(1);
        double parallelUpdateWeight = (Double) tuple.getValue(2);
        if (parallelUpdateWeight != 1.0) {
            weight = MathUtil.times(weight, parallelUpdateWeight);
        }
        if (aggregateWeights == null) {
            aggregateWeights = weight;
        } else {
            MathUtil.plus(aggregateWeights, weight);
        }
        totalUpdateWeight += parallelUpdateWeight;
    }

    public void finishBatch() {
        if (aggregateWeights != null) {
            MathUtil.times(aggregateWeights, 1.0 / totalUpdateWeight);
            LOG.info("New weight vector: " + Arrays.toString(aggregateWeights));
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // TODO Auto-generated method stub

    }

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        // TODO Auto-generated method stub

    }

}
