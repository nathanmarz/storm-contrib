package storm.state;

import java.math.BigInteger;
import java.util.concurrent.Executor;

public interface State {
    void setState(Object snapshot);
    Object getSnapshot();    
    void setExecutor(Executor executor);
    void commit();
    void commit(BigInteger txid);
    void compact();
    void compactAsync();
    void close();
}
