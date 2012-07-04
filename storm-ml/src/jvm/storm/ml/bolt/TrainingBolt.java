package storm.ml.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.CASValue;
import net.spy.memcached.MemcachedClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import storm.ml.Util;

public class TrainingBolt extends BaseBasicBolt {
    Double bias;
    Double threshold;
    Double learning_rate;
    String memcached_servers;
    MemcachedClient memcache;

    public TrainingBolt(Double bias, Double threshold, Double learning_rate, String memcached_servers) {
        this.bias = bias;
        this.threshold = threshold;
        this.learning_rate = learning_rate;
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

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        CASValue<Object> cas_weights = this.memcache.gets("weights");

        List<Double> weights = Util.parse_str_vector((String)cas_weights.getValue());
        List<Double> example = Util.parse_str_vector(tuple.getString(0));

        Double result = Util.dot_product(example, weights) + this.bias;
        int classif = result > this.threshold ? 1 : 0;

        int label = tuple.getInteger(1);
        int error = label - classif;
        if (error != 0) {
            List<Double> new_weights = new ArrayList<Double>(weights);

            for (int i=0; i<example.size(); i++)
                new_weights.set(i, new_weights.get(i) + this.learning_rate * error * example.get(i));

            Long cas_id = cas_weights.getCas();
            this.memcache.cas("weights", cas_id, new_weights.toString());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) { }
}
