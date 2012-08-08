package com.twitter.storm.primitives;

import backtype.storm.ILocalDRPC;
import backtype.storm.drpc.DRPCSpout;
import backtype.storm.drpc.ReturnResults;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.TopologyBuilder;

public class MLTopologyBuilder {

    public static final String MEMCACHED_SERVERS = "127.0.0.1:11211";

    String topology_prefix;

    BaseTrainingSpout training_spout;
    Number training_spout_parallelism;

    IBasicBolt basic_training_bolt;
    IRichBolt rich_training_bolt;
    Number training_bolt_parallelism;

    IBasicBolt basic_evaluation_bolt;
    IRichBolt rich_evaluation_bolt;
    Number evaluation_bolt_parallelism;

    public MLTopologyBuilder(String topologyPrefix) {
        this.topology_prefix = topologyPrefix;
    }

    public TopologyBuilder prepareTopology(String drpcFunctionName, ILocalDRPC drpc) {
        return prepareTopology(drpcFunctionName, drpc, 1.0, 0.0, 0.5, MEMCACHED_SERVERS);
    }

    public void setTrainingSpout(BaseTrainingSpout exampleTrainingSpout, Number parallelism) {
        this.training_spout = exampleTrainingSpout;
        this.training_spout_parallelism = parallelism;
    }

    public void setTrainingSpout(BaseTrainingSpout exampleTrainingSpout) {
        setTrainingSpout(exampleTrainingSpout, 1);
    }

    public void setTrainingBolt(IBasicBolt training_bolt, Number parallelism) {
        this.basic_training_bolt = training_bolt;
        this.rich_training_bolt = null;
        this.training_bolt_parallelism = parallelism;
    }

    public void setTrainingBolt(IBasicBolt training_bolt) {
        setTrainingBolt(training_bolt, 1);
    }

    public void setTrainingBolt(IRichBolt training_bolt, Number parallelism) {
        this.rich_training_bolt = training_bolt;
        this.basic_training_bolt = null;
        this.training_bolt_parallelism = parallelism;
    }

    public void setTrainingBolt(IRichBolt training_bolt) {
        setTrainingBolt(training_bolt, 1);
    }

    public void setEvaluationBolt(IBasicBolt evaluation_bolt, Number parallelism) {
        this.basic_evaluation_bolt = evaluation_bolt;
        this.rich_evaluation_bolt = null;
        this.evaluation_bolt_parallelism = parallelism;
    }

    public void setEvaluationBolt(IBasicBolt evaluation_bolt) {
        setEvaluationBolt(evaluation_bolt, 1);
    }

    public void setEvaluationBolt(IRichBolt evaluation_bolt, Number parallelism) {
        this.rich_evaluation_bolt = evaluation_bolt;
        this.basic_evaluation_bolt = null;
        this.evaluation_bolt_parallelism = parallelism;
    }

    public void setEvaluationBolt(IRichBolt evaluation_bolt) {
        setEvaluationBolt(evaluation_bolt, 1);
    }

    public TopologyBuilder prepareTopology(String drpcFunctionName, ILocalDRPC drpc, double bias, double threshold,
            double learning_rate, String memcached_servers) {
        TopologyBuilder topology_builder = new TopologyBuilder();

        // training
        topology_builder.setSpout(this.topology_prefix + "-training-spout", this.training_spout,
                this.training_spout_parallelism);

        if (this.rich_training_bolt == null) {
            topology_builder.setBolt(this.topology_prefix + "-training-bolt", this.basic_training_bolt,
                    this.training_bolt_parallelism).shuffleGrouping(this.topology_prefix + "-training-spout");
        } else {
            topology_builder.setBolt(this.topology_prefix + "-training-bolt", this.rich_training_bolt,
                    this.training_bolt_parallelism).shuffleGrouping(this.topology_prefix + "-training-spout");
        }
        topology_builder.setBolt("aggregator", new Aggregator(MEMCACHED_SERVERS)).globalGrouping(
                this.topology_prefix + "-training-bolt");

        // evaluation
        DRPCSpout drpc_spout;

        if (drpc != null)
            drpc_spout = new DRPCSpout(drpcFunctionName, drpc);
        else
            drpc_spout = new DRPCSpout(drpcFunctionName);

        topology_builder.setSpout(this.topology_prefix + "-drpc-spout", drpc_spout);

        if (this.rich_evaluation_bolt == null) {
            topology_builder.setBolt(this.topology_prefix + "-drpc-evaluation", this.basic_evaluation_bolt,
                    this.evaluation_bolt_parallelism).shuffleGrouping(this.topology_prefix + "-drpc-spout");
        } else {
            topology_builder.setBolt(this.topology_prefix + "-drpc-evaluation", this.rich_evaluation_bolt,
                    this.evaluation_bolt_parallelism).shuffleGrouping(this.topology_prefix + "-drpc-spout");
        }

        topology_builder.setBolt(this.topology_prefix + "-drpc-return", new ReturnResults()).shuffleGrouping(
                this.topology_prefix + "-drpc-evaluation");
        // return
        return topology_builder;

    }

    public StormTopology createLocalTopology(String drpcFunctionName, ILocalDRPC drpc) {
        return prepareTopology(drpcFunctionName, drpc).createTopology();
    }

    public StormTopology createRemoteTopology(String drpcFunctionName) {
        return prepareTopology(drpcFunctionName, null).createTopology();
    }
}
