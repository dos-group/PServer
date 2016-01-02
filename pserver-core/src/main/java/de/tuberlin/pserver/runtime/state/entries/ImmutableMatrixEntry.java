package de.tuberlin.pserver.runtime.state.entries;

/**
 * Created by hegemon on 01.01.16.
 */
public class ImmutableMatrixEntry<V extends Number> extends MatrixEntry<V> {

    public ImmutableMatrixEntry(long row, long col, V value) {
        super(row, col, value);
    }

    public ImmutableMatrixEntry(MatrixEntry matrixEntry) {
        super(matrixEntry.getRow(), matrixEntry.getCol(), (V) matrixEntry.getValue());
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

}
