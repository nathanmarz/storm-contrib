package com.twitter.storm.primitives;

import java.util.List;
import java.util.Map;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.MemcachedClient;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.twitter.util.Datautil;

public class EvaluationBolt extends BaseBasicBolt {
    Double bias;
    Double threshold;
    String memcached_servers;
    MemcachedClient memcache;

    public EvaluationBolt(Double bias, Double threshold, String memcached_servers) {
        this.threshold = threshold;
        this.bias = bias;
        this.memcached_servers = memcached_servers;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
        try {
            this.memcache = new MemcachedClient(AddrUtil.getAddresses(this.memcached_servers));
        } catch (java.io.IOException e) {
            System.exit(1);
        }
    }

    List<Double> get_latest_weights() {
        String weights = (String) this.memcache.get("weights");
        return Datautil.parse_str_vector(weights);
    }

    public void execute(Tuple tuple, BasicOutputCollector collector) {
        List<Double> weights = get_latest_weights();

        String input_str = tuple.getString(1);
        List<Double> input = Datautil.parse_str_vector(input_str);

        Double result = Datautil.dot_product(input, weights) + bias;

        collector.emit(new Values(tuple.getValue(0), result > this.threshold ? 1 : 0));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "result"));
    }
}
