package com.twitter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

import com.twitter.storm.primitives.LocalLearner;
import com.twitter.storm.primitives.TrainingSpout;

public class MainOnlineTopology {

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("example_spitter", new TrainingSpout());
        builder.setBolt("local_learner", new LocalLearner(2), 1).shuffleGrouping("example_spitter");
        Config conf = new Config();
        conf.setDebug(true);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, builder.createTopology());
        Utils.sleep(10000);
        cluster.killTopology("test");
        cluster.shutdown();

    }
}
