package storm.state.bolt;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.coordination.IBatchBolt;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.transactional.ICommitter;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.tuple.Tuple;
import java.util.Map;
import storm.state.PartitionedState;
import storm.state.State;

public class StatefulTransactionalBoltExecutor implements ICommitter, IBatchBolt<TransactionAttempt> {
    public static String STATE_RESOURCE = StatefulTransactionalBoltExecutor.class.getName() + "/state";
    
    IStatefulTransactionalBolt _delegate;
    String _rootDir;
    State _state;
    TransactionAttempt _attempt;
    BatchOutputCollector _collector;
    
    public StatefulTransactionalBoltExecutor(IStatefulTransactionalBolt delegate, String rootDir) {
        _delegate = delegate;
        _rootDir = rootDir;
    }

    @Override
    public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, TransactionAttempt attempt) {
        _state = (State) context.getTaskData(STATE_RESOURCE);
        if(_state == null) {
            _state = PartitionedState.getState(conf, context, _rootDir, _delegate.getStateBuilder(), _delegate.getSerializations());        
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
        if(!_state.getVersion().equals(_attempt.getTransactionId())) {
            _delegate.updateState(_state);        
        }
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
