package storm.state.example;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class MapExample extends BaseBasicBolt {

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector boc) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
}
