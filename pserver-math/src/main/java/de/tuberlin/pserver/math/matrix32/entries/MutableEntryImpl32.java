package de.tuberlin.pserver.math.matrix32.entries;


public class MutableEntryImpl32 extends EntryImpl32 implements ReusableEntry32 {

    public MutableEntryImpl32(long row, long col, float value) {
        super(row, col, value);
    }

    @Override
    public ReusableEntry32 set(long row, long col, float value) {
        this.row = row;
        this.col = col;
        this.value = value;
        return this;
    }
}
