package storm.state;

import java.math.BigInteger;
import java.util.Map;
import java.util.concurrent.Executor;

public interface IBackingStore {
    void init(Map conf, Serializations sers);
    Object appendAndApply(Transaction transaction, State state);
    void commit(State state);
    void commit(BigInteger txid, State state);
    void compact(State state);
    void compactAsync(State state);
    void setExecutor(Executor executor);
    void resetToLatest(State state);
    void rollback(State state);
    void close();
    BigInteger getVersion();
}
