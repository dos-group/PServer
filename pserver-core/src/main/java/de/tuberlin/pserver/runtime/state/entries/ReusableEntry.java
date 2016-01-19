package de.tuberlin.pserver.runtime.state.entries;

/**
 * Created by hegemon on 01.01.16.
 */
public interface ReusableEntry<V extends Number> extends Entry<V> {

    ReusableEntry<V> set(long row, long col, V value);

}
