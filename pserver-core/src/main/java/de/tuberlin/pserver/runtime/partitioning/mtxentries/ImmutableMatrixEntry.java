package de.tuberlin.pserver.runtime.partitioning.mtxentries;

/**
 * Created by fsander on 30.06.15.
 */
public class ImmutableMatrixEntry extends AbstractMatrixEntry {

    private final long row;
    private final long col;
    private final double value;

    public ImmutableMatrixEntry(MatrixEntry entry) {
        this.row = entry.getRow();
        this.col = entry.getCol();
        this.value = entry.getValue();
    }

    public ImmutableMatrixEntry(long row, long col, double value) {
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
    public double getValue() {
        return value;
    }
}
