package storm.state;

import java.math.BigInteger;

public interface State {
    void setState(Object snapshot);
    Object getSnapshot();
    void commit();
    void commit(BigInteger txid);
    void compact();
    void compactAsync();
    void close();
    void rollback();
    BigInteger getVersion();
}
