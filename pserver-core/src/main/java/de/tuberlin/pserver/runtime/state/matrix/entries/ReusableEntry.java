package de.tuberlin.pserver.runtime.state.matrix.entries;


public interface ReusableEntry<V extends Number> extends Entry<V> {

    ReusableEntry<V> set(long row, long col, V value);
}
