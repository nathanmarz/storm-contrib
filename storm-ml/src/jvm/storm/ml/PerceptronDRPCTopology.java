package storm.ml;

import backtype.storm.Config;
import backtype.storm.drpc.DRPCSpout;
import backtype.storm.drpc.LinearDRPCTopologyBuilder;
import backtype.storm.drpc.ReturnResults;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.internal.OperationFuture;
import net.spy.memcached.MemcachedClient;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;

import storm.ml.bolt.EvaluationBolt;
import storm.ml.bolt.TrainingBolt;
import storm.ml.spout.ExampleTrainingSpout;

public class PerceptronDRPCTopology {
    public static final String MEMCACHED_SERVERS = "127.0.0.1:11211";
    public static final Double bias          = 0.0;
    public static final Double threshold     = 0.1;
    public static final Double learning_rate = 0.1;

    public static TopologyBuilder prepare_topology(LocalDRPC drpc) {
        TopologyBuilder topology_builder = new TopologyBuilder();

        // training
        topology_builder.setSpout("training-spout",
            new ExampleTrainingSpout()
        );

        topology_builder.setBolt("training-bolt",
            new TrainingBolt(
                PerceptronDRPCTopology.bias,
                PerceptronDRPCTopology.threshold,
                PerceptronDRPCTopology.learning_rate,
                PerceptronDRPCTopology.MEMCACHED_SERVERS
            )
        ).shuffleGrouping("training-spout");


        // evaluation
        DRPCSpout drpc_spout; if (drpc!=null)
            drpc_spout = new DRPCSpout("evaluate", drpc);
        else
            drpc_spout = new DRPCSpout("evaluate");

        topology_builder.setSpout("drpc-spout",
            drpc_spout
        );

        topology_builder.setBolt("drpc-evaluation",
            new EvaluationBolt(
                PerceptronDRPCTopology.bias,
                PerceptronDRPCTopology.threshold,
                PerceptronDRPCTopology.MEMCACHED_SERVERS
            )
        ).shuffleGrouping("drpc-spout");

        topology_builder.setBolt(
            "drpc-return",
            new ReturnResults()
        ).shuffleGrouping("drpc-evaluation");


        return topology_builder;
    }

    public static TopologyBuilder prepare_topology() {
        return prepare_topology(null);
    }

    public static void main(String[] args) throws Exception {
        MemcachedClient memcache = new MemcachedClient(AddrUtil.getAddresses(PerceptronDRPCTopology.MEMCACHED_SERVERS));
        OperationFuture promise = memcache.set("weights", 0, "[0.0, 0.0]");
        promise.get();

        Config topoplogy_conf = new Config();

        if (args==null || args.length==0) {
            LocalDRPC drpc = new LocalDRPC();
            LocalCluster cluster = new LocalCluster();

            TopologyBuilder topology_builder = prepare_topology(drpc);
            cluster.submitTopology("perceptron", topoplogy_conf, topology_builder.createTopology());

            int error_count = 0;
            FileWriter fstream = new FileWriter("out.csv");
            BufferedWriter out = new BufferedWriter(fstream);

            List<String> input_vectors = get_input_vectors();
            for (int i=0; i<input_vectors.size(); i++) {
                String input_vector = input_vectors.get(i);

                List<Double> parsed_iv = Util.parse_str_vector(input_vector);
                Double x = parsed_iv.get(0);
                Double y = parsed_iv.get(1);

                String result = drpc.execute("evaluate", input_vector);
                Integer expected_result = ExampleTrainingSpout.get_label(x, y);

                if (!result.equals(expected_result.toString()))
                    error_count += 1;

                Double error_perc = 100*error_count/Double.valueOf(i+1);

                String format = "%s -> %s (expected: %s, %.2f%% error)";
                System.out.println(String.format(format, input_vector, result, expected_result, error_perc));

                format = "%s	%s	%s	%.2f";
                out.write(String.format(format, input_vector, result, expected_result, error_perc));
                out.newLine();
            }
            out.close();

            cluster.shutdown();
            drpc.shutdown();
        } else {
            TopologyBuilder topology_builder = prepare_topology();
            StormSubmitter.submitTopology(args[0], topoplogy_conf, topology_builder.createTopology());
        }
    }

    public static List<String> get_input_vectors() {
        List<String> input_vectors = new ArrayList<String>();
        for (Double x=-10.0; x<=10.0; x+=0.5) {
            for (Double y=-10.0; y<=10.0; y+=0.5) {
                List<Double> result_item = new ArrayList<Double>();
                result_item.add(x);
                result_item.add(y);

                input_vectors.add(result_item.toString());
            }
        }

        return input_vectors;
    }
}
