package storm.ml;

import backtype.storm.Config;
import backtype.storm.drpc.LinearDRPCTopologyBuilder;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;

import java.util.List;

import storm.ml.bolt.EvaluationBolt;
import storm.ml.util.IVParser;

public class PerceptronDRPCTopology {
    public static void main(String[] args) throws Exception {
        LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("evaluate");
        builder.addBolt(new EvaluationBolt(), 3);

        Config conf = new Config();

        if (args==null || args.length==0) {
            LocalDRPC drpc = new LocalDRPC();
            LocalCluster cluster = new LocalCluster();

            cluster.submitTopology("evaluation-demo", conf, builder.createLocalTopology(drpc));

            List<List<Double>> input_vectors = IVParser.parse("input_vectors.txt");
            for (List<Double> input_vector : input_vectors) {
                System.out.println(String.format("%s -> %s", input_vector, drpc.execute("evaluate", input_vector.toString())));
            }

            cluster.shutdown();
            drpc.shutdown();
        } else {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], conf, builder.createRemoteTopology());
        }
    }
}
