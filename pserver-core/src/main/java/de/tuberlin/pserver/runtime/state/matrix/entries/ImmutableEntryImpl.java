package de.tuberlin.pserver.runtime.state.matrix.entries;


public class ImmutableEntryImpl<V extends Number> extends EntryImpl<V> {

    public ImmutableEntryImpl(long row, long col, V value) {
        super(row, col, value);
    }

    public ImmutableEntryImpl(Entry entry) {
        super(entry.getRow(), entry.getCol(), (V) entry.getValue());
    }
}
