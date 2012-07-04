package com.twitter.algorithms;

import java.util.Arrays;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import com.twitter.util.MathUtil;

public class Aggregator extends BaseRichBolt {

    public static Logger LOG = Logger.getLogger(Aggregator.class);
    double[] aggregateWeights = null;
    double totalUpdateWeight = 1.0;

    public void execute(Tuple tuple) {

        double[] weight = (double[]) tuple.getValue(0);
        Double parallelUpdateWeight = (Double) tuple.getValue(1);

        if (parallelUpdateWeight != 1.0) {
            weight = MathUtil.times(weight, parallelUpdateWeight);
        }
        if (aggregateWeights == null) {
            aggregateWeights = weight;
        } else {
            MathUtil.plus(aggregateWeights, weight);
        }
        totalUpdateWeight += parallelUpdateWeight;
        LOG.info("totalUpdate");
        LOG.info(totalUpdateWeight);
        if (aggregateWeights != null) {
            MathUtil.times(aggregateWeights, 1.0 / totalUpdateWeight);
            LOG.info("New AGGREGATE vector: " + Arrays.toString(aggregateWeights));
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

    }

}
