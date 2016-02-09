package de.tuberlin.pserver.math.matrix32.entries;


public abstract class EntryImpl32 implements Entry32 {

    protected long row;
    protected long col;
    protected float value;

    public EntryImpl32(long row, long col, float value) {
        this.row = row;
        this.col = col;
        this.value = value;
    }

    @Override
    public long getRow() {
        return this.row;
    }

    @Override
    public long getCol() {
        return this.col;
    }

    @Override
    public float getValue() {
        return this.value;
    }

    @Override
    public String toString() {
        return this.col + ":" + this.value;
    }
}
