package de.tuberlin.pserver.runtime.state.mtxentries;


public class ImmutableMatrixEntry<V extends Number> extends AbstractMatrixEntry<V> {

    private final long row;
    private final long col;
    private final V value;

    public ImmutableMatrixEntry(MatrixEntry<V> entry) {
        this.row = entry.getRow();
        this.col = entry.getCol();
        this.value = entry.getValue();
    }

    public ImmutableMatrixEntry(long row, long col, V value) {
        this.row = row;
        this.col = col;
        this.value = value;
    }

    @Override
    public long getRow() {
        return row;
    }

    @Override
    public long getCol() {
        return col;
    }

    @Override
    public V getValue() {
        return value;
    }
}
