package com.twitter;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import com.twitter.storm.primitives.LocalLearner;
import com.twitter.storm.primitives.TrainingSpout;
import com.twitter.util.MathUtil;

public class MainOnlineTopology {

    public static List<List<Object>> readExamples(String fileName) throws IOException {
        Scanner in = new Scanner(new File(fileName));
        List<List<Object>> tupleList = new ArrayList<List<Object>>();
        while (in.hasNext()) {
            String line = in.nextLine();
            tupleList.add(new Values(line));
        }
        in.close();
        return tupleList;
    }

    public static void main(String[] args) throws Exception {
        int dimension = MathUtil.nextLikelyPrime(10);
        System.out.println("Using dimension: " + dimension);

        // Map exampleMap = new HashMap<Integer, List<List<Object>>>();
        // exampleMap.put(0, readExamples(args[0]));

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

        // builder.setBolt("local_learner", new LocalLearner(dimension), 1).customGrouping(spout, grouping);
    }
}
