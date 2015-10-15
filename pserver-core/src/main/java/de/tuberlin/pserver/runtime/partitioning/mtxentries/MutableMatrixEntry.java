package de.tuberlin.pserver.runtime.partitioning.mtxentries;

/**
 * Represents a matrix entry with row, cols and value.
 *
 * This implementation is mutable and is useful in situations where an iteration over entries would cause multiple
 * unnecessary object instantiations.
 */
public class MutableMatrixEntry<V extends Number> extends AbstractMatrixEntry<V> implements ReusableMatrixEntry<V> {

    private long row;

    private long col;

    private V value;

    public MutableMatrixEntry(long row, long col, V value) {
        this.row = row;
        this.col = col;
        this.value = value;
    }

    public MutableMatrixEntry set(long row, long col, V value) {
        this.row = row;
        this.col = col;
        this.value = value;
        return this;
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
