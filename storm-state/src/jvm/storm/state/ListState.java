package storm.state;

import clojure.lang.IPersistentVector;
import clojure.lang.PersistentVector;
import clojure.lang.SeqIterator;
import java.math.BigInteger;
import java.util.AbstractList;
import java.util.Iterator;
import java.util.Map;

public class ListState<T> extends AbstractList<T> implements State {
    public static class Factory implements StateFactory {
        @Override
        public State makeState(Map conf, IBackingStore store, Serializations sers) {
            return new ListState(conf, store, sers);
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
    
    IBackingStore _store;
    
    public ListState(Map conf, IBackingStore store) {
        this(conf, store, new Serializations());
    }
    
    public ListState(Map conf, IBackingStore store, Serializations sers) {
        sers = sers.clone();
        sers.add(Set.class).add(Clear.class).add(Add.class); 
        _store = store;
        _store.init(conf, sers);
        _store.resetToLatest(this);
    }
    
    @Override
    public Object getSnapshot() {
        return _cache;
    }
        
    public void commit() {
        _store.commit(this);
    }

    public void commit(BigInteger txid) {
        _store.commit(txid, this);
    }    
    
    public void compact() {
        _store.compact(this);
    }

    public void compactAsync() {
        _store.compactAsync(this);
    }
    
    public void rollback() {
        _store.rollback(this);
    }
    
    @Override
    public void close() {
        _store.close();
    }

    @Override
    public BigInteger getVersion() {
        return _store.getVersion();
    }    

    @Override
    public void clear() {
        _store.appendAndApply(new Clear(), this);
    }
    
    @Override
    public boolean add(T e) {
        return (Boolean) _store.appendAndApply(new Add(e), this);
    }        
    
    @Override
    public T remove(int i) {
        throw new UnsupportedOperationException();
    }    
    
    @Override
    public T set(int i, T e) {
        return (T) _store.appendAndApply(new Set(i, e), this);
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
