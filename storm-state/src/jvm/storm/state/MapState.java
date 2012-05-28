package storm.state;

import clojure.lang.IPersistentMap;
import clojure.lang.MapEntry;
import clojure.lang.PersistentHashMap;
import java.math.BigInteger;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Executor;
import storm.state.HDFSState.State;


public class MapState<K, V> extends AbstractMap<K, V> implements State {
    IPersistentMap _cache;
    HDFSState _state;

    @Override
    public void setState(Object snapshot) {
        if(snapshot==null) {
            _cache = PersistentHashMap.EMPTY;
        } else {
            _cache = (PersistentHashMap) snapshot;            
        }
    }
    
    public static class Put implements Transaction<MapState> {
        Object key;
        Object value;
        
        public Put(Object k, Object v) {
            key = k;
            value = v;
        }

        @Override
        public Object apply(MapState state) {
           Object ret = state._cache.valAt(key);
           state._cache = state._cache.assoc(key, value);
           return ret;
        }
    }
    
    public static class Remove implements Transaction<MapState> {
        Object key;
        
        public Remove(Object k) {
            key = k;
        }

        @Override
        public Object apply(MapState state) {
           Object ret = state._cache.valAt(key);
           state._cache = state._cache.without(key);
           return ret;
        }        
    }

    public static class Clear implements Transaction<MapState> {
        @Override
        public Object apply(MapState state) {
           state._cache = PersistentHashMap.EMPTY;
           return null;
        }        
    }    
    
    public MapState(String fsLocation, Serializations sers) {
        sers = sers.clone();
        sers.add(Put.class).add(Remove.class).add(Clear.class);
        _state = new HDFSState(fsLocation, sers);
        _state.resetToLatest(this);
    }
    
    public void commit() {
        _state.commit();
    }
    
    public void commit(BigInteger txid) {
        _state.commit(txid);
    }    
    
    public void compact(Executor executor) {
        _state.compact(_cache, executor);
    }

    @Override
    public V put(K key, V value) {
        return (V) _state.appendAndApply(new Put(key, value), this);
    }

    @Override
    public V remove(Object key) {
        return (V) _state.appendAndApply(new Remove(key), this);
    }

    @Override
    public void clear() {
        _state.appendAndApply(new Clear(), this);
    }
    
    public Iterator<MapEntry> iterator() {
        return _cache.iterator();
    }
    
    @Override
    public Set<Entry<K, V>> entrySet() {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }    
    
    @Override
    public int size() {
        return _cache.count();
    }

    @Override
    public boolean containsKey(Object o) {
        return _cache.containsKey(o);
    }    
    
    @Override
    public V get(Object key) {
        return (V) _cache.valAt(key);
    }
}
