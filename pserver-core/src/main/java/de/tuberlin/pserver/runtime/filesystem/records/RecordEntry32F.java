package de.tuberlin.pserver.runtime.filesystem.records;


public final class RecordEntry32F {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private long row;

    private long col;

    private float value;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public RecordEntry32F(long row, long col, float value) { set(row, col, value); }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public RecordEntry32F set(long row, long col, float value) {
        this.row = row;
        this.col = col;
        this.value = value;
        return this;
    }

    public long getRow() { return row; }

    public long getCol() { return col; }

    public float getValue() { return value; }

    public String toString() { return col + ":" + value; }
}
