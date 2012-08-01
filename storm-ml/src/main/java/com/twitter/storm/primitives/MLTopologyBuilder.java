package com.twitter.storm.primitives;

import backtype.storm.ILocalDRPC;
import backtype.storm.drpc.DRPCSpout;
import backtype.storm.drpc.ReturnResults;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;

public class MLTopologyBuilder {

    public static final String MEMCACHED_SERVERS = "127.0.0.1:11211";
    private BaseRichBolt trainingBolt;
    private BaseRichSpout trainingSpout;

    public TopologyBuilder prepareTopology(ILocalDRPC drpc) {
        return prepareTopology(drpc, 3.0, 0.0, 3.0, MEMCACHED_SERVERS);
    }

    public void setTrainingBolt(BaseRichBolt trainingBolt) {
        this.trainingBolt = trainingBolt;
    }

    public void setTrainingSpout(BaseRichSpout trainingSpout) {
        this.trainingSpout = trainingSpout;
    }

    public TopologyBuilder prepareTopology(ILocalDRPC drpc, double bias, double threshold, double learning_rate,
            String memcached_servers) {
        TopologyBuilder topology_builder = new TopologyBuilder();

        // training
        topology_builder.setSpout("training-spout", new ExampleTrainingSpout());

        topology_builder.setBolt("training-bolt", new LocalLearner(bias, threshold, learning_rate, MEMCACHED_SERVERS))
                .shuffleGrouping("training-spout");

        // evaluation
        DRPCSpout drpc_spout;
        if (drpc != null)
            drpc_spout = new DRPCSpout("evaluate", drpc);
        else
            drpc_spout = new DRPCSpout("evaluate");

        topology_builder.setSpout("drpc-spout", drpc_spout);

        topology_builder.setBolt(
                "drpc-evaluation",
                new EvaluationBolt(PerceptronDRPCTopology.bias, PerceptronDRPCTopology.threshold,
                        PerceptronDRPCTopology.MEMCACHED_SERVERS)).shuffleGrouping("drpc-spout");

        topology_builder.setBolt("drpc-return", new ReturnResults()).shuffleGrouping("drpc-evaluation");

        // return
        return topology_builder;

    }

    public StormTopology createLocalTopology(ILocalDRPC drpc) {
        return prepareTopology(drpc).createTopology();
    }

    public StormTopology createRemoteTopology() {
        return prepareTopology(null).createTopology();
    }
}
