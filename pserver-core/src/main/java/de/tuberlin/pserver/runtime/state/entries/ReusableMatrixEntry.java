package de.tuberlin.pserver.runtime.state.entries;

/**
 * Created by hegemon on 01.01.16.
 */
public interface ReusableMatrixEntry<V extends Number> extends ReusableEntry<V> {

    Entry set(long row, long col, V value);

}
