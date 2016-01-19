package de.tuberlin.pserver.runtime.state.entries;

/**
 * Created by hegemon on 01.01.16.
 */
public class ImmutableEntryImpl<V extends Number> extends EntryImpl<V> {

    public ImmutableEntryImpl(long row, long col, V value) {
        super(row, col, value);
    }

    public ImmutableEntryImpl(Entry entry) {
        super(entry.getRow(), entry.getCol(), (V) entry.getValue());
    }

}
