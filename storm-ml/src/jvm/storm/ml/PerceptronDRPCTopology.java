package storm.ml;

import backtype.storm.Config;
import backtype.storm.drpc.LinearDRPCTopologyBuilder;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

import java.util.ArrayList;
import java.util.List;

import storm.ml.bolt.EvaluationBolt;
import storm.ml.bolt.TrainingBolt;
import storm.ml.spout.TrainingSpout;

public class PerceptronDRPCTopology {
    public static void main(String[] args) throws Exception {
        Double bias          = 1.0;
        Double threshold     = 0.5;
        Double learning_rate = 0.2;

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("training-spout", new TrainingSpout(), 10);
        builder.setBolt("training-bolt", new TrainingBolt(bias, threshold, learning_rate), 3)
               .shuffleGrouping("training-spout");

        LinearDRPCTopologyBuilder drpc_builder = new LinearDRPCTopologyBuilder("evaluate");
        drpc_builder.addBolt(new EvaluationBolt(bias, threshold), 3);

        Config conf = new Config();

        if (args==null || args.length==0) {
            LocalDRPC drpc = new LocalDRPC();
            LocalCluster cluster = new LocalCluster();

            cluster.submitTopology("training-demo", conf, builder.createTopology());
            cluster.submitTopology("evaluation-demo", conf, drpc_builder.createLocalTopology(drpc));

            List<String> input_vectors = get_input_vectors();
            for (String input_vector : input_vectors) {
                System.out.println(String.format("%s -> %s", input_vector, drpc.execute("evaluate", input_vector)));
            }

            cluster.shutdown();
            drpc.shutdown();
        } else {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
            StormSubmitter.submitTopology(args[0], conf, drpc_builder.createRemoteTopology());
        }
    }

    public static List<String> get_input_vectors() {
        List<String> input_vectors = new ArrayList<String>();
        for (Double x=-10.0; x<=10.0; x++) {
            for (Double y=-10.0; y<=10.0; y++) {
                List<Double> result_item = new ArrayList<Double>();
                result_item.add(x);
                result_item.add(y);

                input_vectors.add(result_item.toString());
            }
        }

        return input_vectors;
    }
}
