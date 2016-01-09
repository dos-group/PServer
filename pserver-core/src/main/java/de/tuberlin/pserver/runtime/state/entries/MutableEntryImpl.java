package de.tuberlin.pserver.runtime.state.entries;

import java.util.Map;

/**
 * Created by hegemon on 01.01.16.
 */
public class MutableEntryImpl<V extends Number> extends EntryImpl<V> implements ReusableEntry<V> {

    public MutableEntryImpl(long row, long col, V value) {
        super(row, col, value);
    }

    @Override
    public ReusableEntry<V> set(long row, long col, V value) {
        this.row = row;
        this.col = col;
        this.value = value;
        return this;
    }

}
