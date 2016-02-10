package de.tuberlin.pserver.types.matrix.implementation.f32.entries;


public class ImmutableEntryImpl32F extends EntryImpl32F {

    public ImmutableEntryImpl32F(long row, long col, float value) {
        super(row, col, value);
    }

    public ImmutableEntryImpl32F(Entry32F entry) {
        super(entry.getRow(), entry.getCol(), entry.getValue());
    }
}
