// Copyright (c) P. Taylor Goetz (ptgoetz@gmail.com)

package backtype.storm.contrib.signals.test;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class SignalTopology {

    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("signal-spout", new TestSignalSpout("test-signal-spout"));

        Config conf = new Config();
        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, builder.createTopology());
        Utils.sleep(120000);
        cluster.killTopology("test");
        cluster.shutdown();
    }

}
