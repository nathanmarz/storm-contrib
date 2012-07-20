package com.twitter.storm.primitives;

import java.util.List;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import com.twitter.util.Datautil;

public class TrainingSpout extends BaseRichSpout {
    SpoutOutputCollector _collector;

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
    }

    public void nextTuple() {
        Utils.sleep(100);
        List<Double[]> dataSet = new Datautil().readTrainingFile();
        for (Double[] trainingItem : dataSet) {
            _collector.emit(new Values(trainingItem));
        }

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("trainingItem1", "t2", "t3"));
    }

}
