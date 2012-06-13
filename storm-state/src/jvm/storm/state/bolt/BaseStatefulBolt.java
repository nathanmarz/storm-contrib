package storm.state.bolt;

import backtype.storm.topology.BasicOutputCollector;
import java.util.Map;
import storm.state.Serializations;


public abstract class BaseStatefulBolt implements IStatefulBolt {

    @Override
    public void preCommit(BasicOutputCollector collector) {
    }

    @Override
    public void cleanup() {
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public Serializations getSerializations() {
        return new Serializations();
    }
    
}
