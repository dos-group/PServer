package de.tuberlin.pserver.math.matrix32.entries;


public class ImmutableEntryImpl32 extends EntryImpl32 {

    public ImmutableEntryImpl32(long row, long col, float value) {
        super(row, col, value);
    }

    public ImmutableEntryImpl32(Entry32 entry) {
        super(entry.getRow(), entry.getCol(), entry.getValue());
    }
}
