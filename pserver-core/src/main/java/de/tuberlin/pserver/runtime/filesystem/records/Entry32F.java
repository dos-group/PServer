package de.tuberlin.pserver.runtime.filesystem.records;


public class Entry32F {

    private long row;
    private long col;
    private float value;

    public Entry32F(long row, long col, float value) {
        set(row, col, value);
    }

    public Entry32F set(long row, long col, float value) {
        this.row = row;
        this.col = col;
        this.value = value;
        return this;
    }

    public long getRow() {
        return this.row;
    }

    public long getCol() {
        return this.col;
    }

    public float getValue() {
        return this.value;
    }

    public String toString() {
        return this.col + ":" + this.value;
    }
}
