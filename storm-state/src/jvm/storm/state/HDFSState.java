package storm.state;

import carbonite.JavaBridge;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.DefaultSerializers.BigIntegerSerializer;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;


public class HDFSState {
    public static interface State {
        public void setState(Object snapshot);
    }
    
    List<Transaction> _pendingTransactions = new ArrayList<Transaction>();
    Kryo _serializer;
    
    public HDFSState(String dfsDir, Serializations sers) {
        _serializer = new Kryo();
        sers.apply(_serializer);
        _serializer.register(Commit.class);
        _serializer.register(BigInteger.class, new BigIntegerSerializer());
        try {
            //automatically support clojure collections since those are common
            JavaBridge.registerCollections(_serializer);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    public Object appendAndApply(Transaction entry, State state) {
        _pendingTransactions.add(entry);
        return entry.apply(state);
    }
    
    public void commit() {
        commit(null);
    }
    
    public void commit(BigInteger txid) {
        Commit commit = new Commit(txid, _pendingTransactions);
        _pendingTransactions = new ArrayList<Transaction>();
        // TODO: write to open txlog  
        // TODO: finish
    }
    
    public void resetToLatest(State state) {
        // read the most recent snapshot
        // then for all unapplied transactions, apply them
    }

    
    public void compact(Object immutableSnapshot, Executor executor) {
        //TOOD: need to do this in the background... need an executor threadpool that can be used
    }    
    
    private static class Commit {
        BigInteger txid;
        List<Transaction> transactions;
        
        public Commit(BigInteger txid, List<Transaction> transactions) {
            this.txid = txid;
            this.transactions = transactions;
        }
        
    }
}
