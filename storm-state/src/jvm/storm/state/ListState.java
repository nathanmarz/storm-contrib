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
import storm.state.hdfs.HDFSState.State;

public class ListState<T> extends AbstractList<T> implements State {
    public static ListState openPartition(Map conf, TopologyContext context, String stateDir, Serializations sers) {
        ListState ret = new ListState(PartitionedState.thisStateDir(conf, context, stateDir), sers);
        ret.setExecutor(context.getSharedExecutor());
        return ret;
    }

    public static ListState openPartition(Map conf, TopologyContext context, String stateDir) {
        return openPartition(conf, context, stateDir, new Serializations());
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
    
    public ListState(String dfsDir) {
        this(dfsDir, new Serializations());
    }
    
    public ListState(String dfsDir, Serializations sers) {
        sers = sers.clone();
        sers.add(Set.class).add(Clear.class).add(Add.class);        
        _state = new HDFSState(dfsDir, sers);
        _state.resetToLatest(this);
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
    
    public void compact(Executor executor) {
        _state.compact(_cache);//executor);
    }

    public void compactAsync() {
        _state.compactAsync(_cache);
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
