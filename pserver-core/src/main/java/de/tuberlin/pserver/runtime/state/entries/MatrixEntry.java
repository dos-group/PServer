package de.tuberlin.pserver.runtime.state.entries;

/**
 * Created by hegemon on 01.01.16.
 */
public abstract class MatrixEntry<V extends Number> implements Entry<V> {

    protected long row;
    protected long col;
    protected V value;

    public MatrixEntry(long row, long col, V value) {
        this.row = row;
        this.col = col;
        this.value = value;
    }

    abstract public long getRow();

    abstract public long getCol();

    abstract public V getValue();

    @Override
    public String toString() {
        return "(" + getRow() + ", " + getCol() + ", " + getValue() + ")";
    }

}
