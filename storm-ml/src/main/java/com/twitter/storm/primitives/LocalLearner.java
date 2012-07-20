package com.twitter.storm.primitives;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.MemcachedClient;

import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.transactional.ICommitter;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.twitter.algorithms.Learner;
import com.twitter.data.Example;
import com.twitter.data.HashAll;

public class LocalLearner extends BaseRichBolt implements ICommitter {
    public static Logger LOG = Logger.getLogger(LocalLearner.class);

    private int dimension;
    OutputCollector _collector;
    List<Example> buffer = new ArrayList<Example>();
    Object id;
    OutputCollector collector;
    HashAll hashFunction;
    Learner learner;
    double[] weightVector;
    MemcachedClient memcache;

    public LocalLearner(int dimension, String memcached_servers) throws IOException {
        this(dimension, new Learner(dimension, new MemcachedClient(AddrUtil.getAddresses(memcached_servers))));
    }

    public LocalLearner(int dimension, Learner onlinePerceptron) {// , HashAll hashAll) {
        try {
            this.dimension = dimension;
            this.learner = onlinePerceptron;
            // this.hashFunction = hashAll;

            weightVector = new double[dimension];
            weightVector = new double[dimension];
            weightVector[0] = -6.8;
            weightVector[1] = -0.8;
            learner.setWeights(weightVector);
        } catch (Exception e) {

        }
    }

    public void execute(Tuple tuple) {
        Example example = new Example(2);
        example.x[0] = (Double) tuple.getValue(0);
        example.x[1] = (Double) tuple.getValue(1);
        example.label = (Double) tuple.getValue(2);
        learner.update(example, 1);
        _collector.emit(Arrays.asList((Object) learner.getWeights(), (Object) learner.getParallelUpdateWeight()));
        _collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("weight_vector", "parallel_weight"));
    }

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        learner.initWeights(weightVector);
        _collector = collector;
        weightVector = (double[]) context.getTaskData();
        context.setTaskData(weightVector);
    }
}
