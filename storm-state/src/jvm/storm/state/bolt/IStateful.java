package storm.state.bolt;

import storm.state.Serializations;
import storm.state.StateFactory;

public interface IStateful {
    StateFactory getStateBuilder();
    Serializations getSerializations();    
}
