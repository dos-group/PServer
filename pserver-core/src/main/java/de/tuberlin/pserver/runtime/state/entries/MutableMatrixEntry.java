package de.tuberlin.pserver.runtime.state.entries;

/**
 * Created by hegemon on 01.01.16.
 */
public class MutableMatrixEntry<V extends Number> extends MatrixEntry<V> implements ReusableMatrixEntry<V> {

    public MutableMatrixEntry(long row, long col, V value) {
        super(row, col, value);
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
    public V getValue() {
        return this.value;
    }

    @Override
    public Entry set(long row, long col, V value) {
        this.row = row;
        this.col = col;
        this.value = value;
        return this;
    }

}
