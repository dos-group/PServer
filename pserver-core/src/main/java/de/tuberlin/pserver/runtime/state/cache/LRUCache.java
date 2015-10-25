package de.tuberlin.pserver.runtime.state.cache;

import java.util.LinkedHashMap;
import java.util.Map;

public class LRUCache<K, V> extends LinkedHashMap<K, V> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private int cacheSize;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public LRUCache(final int cacheSize) {
        super(16, 0.75f, true);
        this.cacheSize = cacheSize;
    }

    // ---------------------------------------------------
    // Protected Methods.
    // ---------------------------------------------------

    protected boolean removeEldestEntry(final Map.Entry<K, V> eldest) {
        return size() >= cacheSize;
    }
}