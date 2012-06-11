package storm.state;

import backtype.storm.task.TopologyContext;
import clojure.lang.IPersistentVector;
import clojure.lang.PersistentVector;
import clojure.lang.SeqIterator;
import java.math.BigInteger;
import java.util.AbstractList;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Executor;
import storm.state.hdfs.HDFSState;

public class ListState<T> extends AbstractList<T> implements State {
    public static class Factory implements StateFactory {
        @Override
        public State makeState(Map conf, String rootDir, Serializations sers) {
            return new ListState(conf, rootDir, sers);
        }        
    }
        
    IPersistentVector _cache;
    
    public static class Clear implements Transaction<ListState> {

        @Override
        public Object apply(ListState state) {
            state._cache = PersistentVector.EMPTY;
            return null;
        }        
    }
    
    public static class Add implements Transaction<ListState> {
        Object obj;
        
        //for kryo
        public Add() {
            
        }
        
        public Add(Object o) {
            obj = o;
        }

        @Override
        public Object apply(ListState state) {
            state._cache = state._cache.cons(obj);
            return true;
        }
    }
    
    public static class Set implements Transaction<ListState> {
        int i;
        Object obj;
        
        //for kryo
        public Set() {
            
        }
        
        public Set(int i, Object o) {
            this.i = i;
            this.obj = o;
        }

        @Override
        public Object apply(ListState state) {
            Object ret = state._cache.nth(i);
            state._cache = state._cache.assocN(i, obj);
            return ret;
        }
    }
    
    HDFSState _state;
    
    public ListState(Map conf, String dfsDir) {
        this(conf, dfsDir, new Serializations());
    }
    
    public ListState(Map conf, String dfsDir, Serializations sers) {
        sers = sers.clone();
        sers.add(Set.class).add(Clear.class).add(Add.class);        
        _state = new HDFSState(conf, dfsDir, sers);
        _state.resetToLatest(this);
    }
    
    @Override
    public Object getSnapshot() {
        return _cache;
    }
    
    public void setExecutor(Executor e) {
        _state.setExecutor(e);
    }
    
    public void commit() {
        _state.commit(this);
    }

    public void commit(BigInteger txid) {
        _state.commit(txid, this);
    }    
    
    public void compact() {
        _state.compact(this);
    }

    public void compactAsync() {
        _state.compactAsync(this);
    }    
    
    @Override
    public void close() {
        _state.close();
    }

    @Override
    public void clear() {
        _state.appendAndApply(new Clear(), this);
    }
    
    @Override
    public boolean add(T e) {
        return (Boolean) _state.appendAndApply(new Add(e), this);
    }        
    
    @Override
    public T remove(int i) {
        throw new UnsupportedOperationException();
    }    
    
    @Override
    public T set(int i, T e) {
        return (T) _state.appendAndApply(new Set(i, e), this);
    }
    
    @Override
    public void setState(Object snapshot) {
        if(snapshot==null) {
            _cache = PersistentVector.EMPTY;
        } else {
            _cache = (IPersistentVector) snapshot;            
        }
    }

    @Override
    public int size() {
        return _cache.length();
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public Iterator<T> iterator() {
        return new SeqIterator(_cache.seq());
    }

    @Override
    public T get(int i) {
        return (T) _cache.nth(i);
    }
    
}
