// Copyright (c) P. Taylor Goetz (ptgoetz@gmail.com)

package backtype.storm.contrib.signals.test;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.contrib.signals.spout.BaseSignalSpout;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;

public class TestSignalSpout extends BaseSignalSpout {


    private static final Logger LOG = LoggerFactory.getLogger(TestSignalSpout.class);

    public TestSignalSpout(String name) {
        super(name);
    }

    @Override
    public void onSignal(byte[] data) {
        LOG.info("Received signal: " + new String(data));

    }

    @Override
    public void nextTuple() {
        // TODO Auto-generated method stub

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // TODO Auto-generated method stub

    }

}
