package storm.state.example;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class MapExample extends BaseBasicBolt {
    // TODO: create a "StatefulBolt" that auto-acks / commits based on a time interval (use tick tuples for this)
    // use conf to specify how frequently compaction should be triggered
    // stateful bolt will be parameterized with the state in prepare
    // also need a stateful transactional committer bolt that passes in the transaction id to commit in finishBatch
    // decides whether to auto-compact based on the same config after each finishBatch
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector boc) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
}
