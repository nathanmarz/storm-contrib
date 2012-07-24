package storm.ml.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.MemcachedClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import storm.ml.Util;

public class EvaluationBolt extends BaseBasicBolt {
    Double bias;
    Double threshold;
    String memcached_servers;
    MemcachedClient memcache;

    public EvaluationBolt(Double bias, Double threshold, String memcached_servers) {
        this.bias = bias;
        this.threshold = threshold;
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
        String weights = (String)this.memcache.get("weights");
        return Util.parse_str_vector(weights);
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String input_str = tuple.getString(0);
        Object retInfo = tuple.getValue(1);

        List<Double> weights = get_latest_weights();
        List<Double> input = Util.parse_str_vector(input_str);

        Double evaluation = Util.dot_product(input, weights) + this.bias;
        String result = evaluation > this.threshold ? "1" : "0";

        collector.emit(new Values(result, retInfo));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "result"));
    }
}
