package storm.state.bolt;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import storm.state.IPartitionedBackingStore;
import storm.state.PartitionedState;
import storm.state.State;


public class StatefulBoltExecutor implements IRichBolt {
    IStatefulBolt _delegate;
    State _state;
    transient BasicOutputCollector _collector;
    OutputCollector _rootCollector;
    Boolean _immediateAck;
    List<Tuple> _pendingAcks = new ArrayList<Tuple>();
    IPartitionedBackingStore _store;
    
    public StatefulBoltExecutor(IStatefulBolt delegate, IPartitionedBackingStore store) {
        _delegate = delegate;
        _store = store;
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _state = PartitionedState.getState(conf, context, _store, _delegate.getStateBuilder(), _delegate.getSerializations());
        _delegate.prepare(conf, context, _state);
        _rootCollector = collector;
        _collector = new BasicOutputCollector(collector);
        _immediateAck = (Boolean) conf.get(IStatefulBolt.TOPOLOGY_STATE_IMMEDIATE_ACK);
        if(_immediateAck == null) _immediateAck = false;
    }

    @Override
    public void execute(Tuple tuple) {
        if(tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID)) {
            _delegate.preCommit(_collector);
            _state.commit();
            for(Tuple t: _pendingAcks) {
                _rootCollector.ack(t);
            }
            _pendingAcks.clear();
        } else {
            _delegate.execute(tuple, _collector);        
        }
        if(_immediateAck) {
            _rootCollector.ack(tuple);        
        } else {
            _pendingAcks.add(tuple);
        }
    }

    @Override
    public void cleanup() {
        _delegate.cleanup();
        _state.close();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        _delegate.declareOutputFields(declarer);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map ret = _delegate.getComponentConfiguration();
        if(ret==null) ret = new HashMap();
        Number commitFreq = (Number) ret.get(IStatefulBolt.TOPOLOGY_STATE_COMMIT_FREQ_SECS);
        if(commitFreq == null) commitFreq = 1;
        ret.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, commitFreq.intValue());
        return ret;
    }
}
