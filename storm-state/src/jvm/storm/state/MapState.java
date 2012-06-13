package storm.state;

import clojure.lang.IPersistentMap;
import clojure.lang.MapEntry;
import clojure.lang.PersistentHashMap;
import java.math.BigInteger;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;


public class MapState<K, V> extends AbstractMap<K, V> implements State {    
    public static class Factory implements StateFactory {         
        @Override
        public State makeState(Map conf, IBackingStore store, Serializations sers) {
            return new MapState(conf, store, sers);
        }        
    }  
    
    IPersistentMap _cache;
    IBackingStore _store;

    @Override
    public void setState(Object snapshot) {
        if(snapshot==null) {
            _cache = PersistentHashMap.EMPTY;
        } else {
            _cache = (IPersistentMap) snapshot;            
        }
    }
    
    @Override
    public Object getSnapshot() {
        return _cache;
    }
    
    public static class Put implements Transaction<MapState> {
        Object key;
        Object value;
        
        //for kryo
        public Put() {
            
        }
        
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
        
        //for kryo
        public Remove() {
            
        }
        
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
    
    public MapState(Map conf, IBackingStore store) {
        this(conf, store, new Serializations());
    }
    
    public MapState(Map conf, IBackingStore store, Serializations sers) {        
        _store = store;
        _store.init(conf, sers);
        _store.resetToLatest(this);
    }
    
    public static Serializations getSers(Serializations base) {
        Serializations ret = base.clone();
        return ret.add(Put.class).add(Remove.class).add(Clear.class);
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
    public V put(K key, V value) {
        return (V) _store.appendAndApply(new Put(key, value), this);
    }

    @Override
    public V remove(Object key) {
        return (V) _store.appendAndApply(new Remove(key), this);
    }

    @Override
    public void clear() {
        _store.appendAndApply(new Clear(), this);
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
