package com.twitter.storm.primitives;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.MemcachedClient;

import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import com.twitter.util.Datautil;
import com.twitter.util.MathUtil;

public class Aggregator extends BaseRichBolt {

    public static Logger LOG = Logger.getLogger(Aggregator.class);
    List<Double> aggregateWeights = null;
    double totalUpdateWeight = 1.0;
    MemcachedClient memcache;
    String memcached_servers;

    public Aggregator(String memcached_servers) {
        this.memcached_servers = memcached_servers;
    }

    public void execute(Tuple tuple) {

        List<Double> weight = (List<Double>) tuple.getValue(0);
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
        MathUtil.times(aggregateWeights, 1.0 / totalUpdateWeight);
        if (aggregateWeights != null) {
            memcache.set("model", 3600 * 24, Datautil.toStrVector(aggregateWeights));
        }

    }

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        try {
            memcache = new MemcachedClient(AddrUtil.getAddresses(memcached_servers));
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // TODO Auto-generated method stub

    }
}
