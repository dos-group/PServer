package de.tuberlin.pserver.types.matrix.implementation.f32.entries;


public class MutableEntryImpl32F extends EntryImpl32F implements ReusableEntry32F {

    public MutableEntryImpl32F(long row, long col, float value) {
        super(row, col, value);
    }

    @Override
    public ReusableEntry32F set(long row, long col, float value) {
        this.row = row;
        this.col = col;
        this.value = value;
        return this;
    }
}
