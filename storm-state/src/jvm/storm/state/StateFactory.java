package storm.state;

import java.io.Serializable;
import java.util.Map;


public interface StateFactory extends Serializable {
    State makeState(Map conf, String rootDir, Serializations sers);
}
