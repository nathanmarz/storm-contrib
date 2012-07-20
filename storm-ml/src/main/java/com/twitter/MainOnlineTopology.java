package com.twitter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.drpc.LinearDRPCTopologyBuilder;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

import com.twitter.algorithms.Aggregator;
import com.twitter.storm.primitives.EvaluationBolt;
import com.twitter.storm.primitives.LocalLearner;
import com.twitter.storm.primitives.TrainingSpout;

public class MainOnlineTopology {
    public static final String MEMCACHED_SERVERS = "127.0.0.1:11211";
    static Double threshold = 0.5;
    static Double bias = 1.0;

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();
        LocalDRPC drpc = new LocalDRPC();

        builder.setSpout("example_spitter", new TrainingSpout());
        builder.setBolt("local_learner", new LocalLearner(2, MEMCACHED_SERVERS), 1).shuffleGrouping("example_spitter");
        builder.setBolt("aggregator", new Aggregator()).globalGrouping("local_learner");

        LinearDRPCTopologyBuilder drpc_builder = new LinearDRPCTopologyBuilder("evaluate");
        drpc_builder.addBolt(new EvaluationBolt(bias, threshold, MEMCACHED_SERVERS), 3);

        Config conf = new Config();
        conf.setDebug(true);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("learning", conf, builder.createTopology());
        cluster.submitTopology("evaluation", conf, drpc_builder.createLocalTopology(drpc));

        Utils.sleep(10000);
        cluster.killTopology("test");
        cluster.shutdown();

    }
}
