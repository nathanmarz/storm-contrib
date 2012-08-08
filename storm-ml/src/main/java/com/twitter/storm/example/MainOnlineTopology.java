package com.twitter.storm.example;

import java.util.ArrayList;
import java.util.List;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.OperationFuture;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.utils.Utils;

import com.twitter.storm.primitives.EvaluationBolt;
import com.twitter.storm.primitives.MLTopologyBuilder;
import com.twitter.storm.primitives.example.ExampleTrainingSpout;
import com.twitter.storm.primitives.example.LocalLearner;

public class MainOnlineTopology {
    public static final String MEMCACHED_SERVERS = "127.0.0.1:11211";
    public static Logger LOG = Logger.getLogger(MainOnlineTopology.class);
    static Double threshold = 0.5;
    static Double bias = 1.0;

    public static void main(String[] args) throws Exception {
        MemcachedClient memcache = new MemcachedClient(AddrUtil.getAddresses(MEMCACHED_SERVERS));
        OperationFuture promise = memcache.set("model", 0, "[0.1, -0.1]");
        promise.get();

        Config topology_conf = new Config();
        String topology_name;
        if (args == null || args.length == 0)
            topology_name = "perceptron";
        else
            topology_name = args[0];

        MLTopologyBuilder ml_topology_builder = new MLTopologyBuilder(topology_name, MEMCACHED_SERVERS);

        ml_topology_builder.setTrainingSpout(new ExampleTrainingSpout());
        ml_topology_builder.setTrainingBolt(new LocalLearner(2, MEMCACHED_SERVERS));
        ml_topology_builder.setEvaluationBolt(new EvaluationBolt(1.0, 0.0, MEMCACHED_SERVERS));

        if (args == null || args.length == 0) {
            LocalDRPC drpc = new LocalDRPC();
            LocalCluster cluster = new LocalCluster();

            cluster.submitTopology(topology_name, topology_conf,
                    ml_topology_builder.createLocalTopology("evaluate", drpc));

            List<Double> testVector = new ArrayList<Double>();
            testVector.add(3.0);
            testVector.add(1.0);
            String result = drpc.execute("evaluate", testVector.toString());
            LOG.error("RESULT: " + result);

            Utils.sleep(10000);
            cluster.killTopology("perceptron");
            cluster.shutdown();
        }
    }
}
