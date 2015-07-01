package de.tuberlin.pserver.app.types;

import de.tuberlin.pserver.app.types.MatrixEntry;

/**
 * Created by fsander on 30.06.15.
 */
public class ImmutableMatrixEntry implements MatrixEntry {

    private final long row;
    private final long col;
    private final double values;

    public ImmutableMatrixEntry(MatrixEntry entry) {
        this.row = entry.getRow();
        this.col = entry.getCol();
        this.values = entry.getValue();
    }

    public ImmutableMatrixEntry(long row, long col, double values) {
        this.row = row;
        this.col = col;
        this.values = values;
    }

    @Override
    public long getRow() {
        return 0;
    }

    @Override
    public long getCol() {
        return 0;
    }

    @Override
    public double getValue() {
        return 0;
    }
}
