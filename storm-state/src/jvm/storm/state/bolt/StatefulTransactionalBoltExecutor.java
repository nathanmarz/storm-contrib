package storm.state.bolt;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.coordination.IBatchBolt;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.transactional.ICommitter;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.tuple.Tuple;
import java.util.Map;
import storm.state.IPartitionedBackingStore;
import storm.state.PartitionedState;
import storm.state.State;

public class StatefulTransactionalBoltExecutor implements ICommitter, IBatchBolt<TransactionAttempt> {
    public static String STATE_RESOURCE = StatefulTransactionalBoltExecutor.class.getName() + "/state";
    
    IStatefulTransactionalBolt _delegate;
    State _state;
    TransactionAttempt _attempt;
    BatchOutputCollector _collector;
    IPartitionedBackingStore _store;
    
    public StatefulTransactionalBoltExecutor(IStatefulTransactionalBolt delegate, IPartitionedBackingStore store) {
        _delegate = delegate;
        _store = store;
    }

    @Override
    public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, TransactionAttempt attempt) {
        _state = (State) context.getTaskData(STATE_RESOURCE);
        if(_state == null) {
            _state = PartitionedState.getState(conf, context, _store, _delegate.getStateBuilder(), _delegate.getSerializations());        
            context.setTaskData(STATE_RESOURCE, _state);
        }
        _delegate.prepare(conf, context, attempt);
        _attempt = attempt;
        _collector = collector;
        
    }

    @Override
    public void execute(Tuple tuple) {
        _delegate.execute(tuple);
    }

    @Override
    public void finishBatch() {
        if(_state.getVersion()!=null && _state.getVersion().equals(_attempt.getTransactionId())) {
            _state.rollback();
        }
        _delegate.updateState(_state);
        _state.commit(_attempt.getTransactionId());
        _delegate.finishBatch(_state, _collector);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        _delegate.declareOutputFields(declarer);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return _delegate.getComponentConfiguration();
    }   
}
