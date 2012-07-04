package storm.ml;

import backtype.storm.Config;
import backtype.storm.drpc.LinearDRPCTopologyBuilder;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;

import java.util.ArrayList;
import java.util.List;

import storm.ml.bolt.EvaluationBolt;

public class PerceptronDRPCTopology {
    public static void main(String[] args) throws Exception {
        LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("evaluate");
        builder.addBolt(new EvaluationBolt(1.0, 0.5), 3);

        Config conf = new Config();

        if (args==null || args.length==0) {
            LocalDRPC drpc = new LocalDRPC();
            LocalCluster cluster = new LocalCluster();

            cluster.submitTopology("evaluation-demo", conf, builder.createLocalTopology(drpc));

            List<String> input_vectors = get_input_vectors();
            for (String input_vector : input_vectors) {
                System.out.println(String.format("%s -> %s", input_vector, drpc.execute("evaluate", input_vector)));
            }

            cluster.shutdown();
            drpc.shutdown();
        } else {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], conf, builder.createRemoteTopology());
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
