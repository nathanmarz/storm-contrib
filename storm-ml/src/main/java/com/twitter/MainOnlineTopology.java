package com.twitter;

import java.util.ArrayList;
import java.util.List;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.OperationFuture;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.utils.Utils;

import com.twitter.storm.primitives.EvaluationBolt;
import com.twitter.storm.primitives.LocalLearner;
import com.twitter.storm.primitives.MLTopologyBuilder;
import com.twitter.storm.primitives.TrainingSpout;

public class MainOnlineTopology {
    public static final String MEMCACHED_SERVERS = "127.0.0.1:11211";
    static Double threshold = 0.5;
    static Double bias = 1.0;

    public static void main(String[] args) throws Exception {
        MemcachedClient memcache = new MemcachedClient(AddrUtil.getAddresses(MEMCACHED_SERVERS));
        OperationFuture promise = memcache.set("model", 0, "[0.0, 0.0]");
        promise.get();

        Config topology_conf = new Config();
        String topology_name;
        if (args == null || args.length == 0)
            topology_name = "perceptron";
        else
            topology_name = args[0];

        MLTopologyBuilder ml_topology_builder = new MLTopologyBuilder(topology_name);

        ml_topology_builder.setTrainingSpout(new TrainingSpout());
        ml_topology_builder.setTrainingBolt(new LocalLearner(2, MEMCACHED_SERVERS));
        ml_topology_builder.setEvaluationBolt(new EvaluationBolt(1.0, 2.0, MEMCACHED_SERVERS));

        if (args == null || args.length == 0) {
            LocalDRPC drpc = new LocalDRPC();
            LocalCluster cluster = new LocalCluster();

            cluster.submitTopology(topology_name, topology_conf,
                    ml_topology_builder.createLocalTopology("evaluate", drpc));

            Utils.sleep(10000);
            cluster.killTopology("perceptron");
            cluster.shutdown();
        }
    }
}
