package storm.ml.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

import java.util.Map;

public abstract class BaseTrainingSpout extends BaseRichSpout {
    SpoutOutputCollector _collector;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this._collector = collector;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("example", "label"));
    }
}
