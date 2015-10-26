package de.tuberlin.pserver.runtime.state.cache;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class LRUCache<K, V> extends LinkedHashMap<K, V> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final int cacheSize;

    private final List<K> dirtyEntries;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public LRUCache(final int cacheSize) {
        super(16, 0.75f, true);

        this.cacheSize = cacheSize;

        this.dirtyEntries = new ArrayList<>();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public V put(final K key, final V value) {

        dirtyEntries.add(key);

        return super.put(key, value);
    }

    public V putIfAbsent(final K key, final V value) {

        dirtyEntries.add(key);

        return super.putIfAbsent(key, value);
    }

    // ---------------------------------------------------

    public List<K> getDirtyEntryKeys() {

        return dirtyEntries;
    }

    public void clearDirtyEntries() {

        dirtyEntries.clear();
    }

    // ---------------------------------------------------
    // Protected Methods.
    // ---------------------------------------------------

    protected boolean removeEldestEntry(final Map.Entry<K, V> eldest) {
        return size() >= cacheSize;
    }
}