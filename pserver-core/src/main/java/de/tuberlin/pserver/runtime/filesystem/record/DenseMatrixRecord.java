package de.tuberlin.pserver.runtime.filesystem.record;

import de.tuberlin.pserver.runtime.state.mtxentries.ImmutableMatrixEntry;
import de.tuberlin.pserver.runtime.state.mtxentries.MatrixEntry;
import de.tuberlin.pserver.runtime.state.mtxentries.ReusableMatrixEntry;

/**
 * Created by Morgan K. Geldenhuys on 15.12.15.
 */
public class DenseMatrixRecord extends MatrixRecord {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private int[] projection;
    private long row;
    private int currentIndex;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public DenseMatrixRecord(String[] record, int[] projection, long row) {
        super(record);
        this.projection = projection;
        this.row = row;
        this.currentIndex = 0;
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private double getValue(int i) {
        if(projection != null)
            i = projection[currentIndex];

        double result = Double.NaN;
        try {
            result = Double.parseDouble(this.record[i]);
        }
        catch(NumberFormatException e) {}
        this.currentIndex++;
        return result;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public int size() {
        return this.projection != null ? this.projection.length : this.record.length;
    }

    @Override
    public MatrixEntry get(int i) {
        return new ImmutableMatrixEntry(this.row, this.currentIndex, this.getValue(i));
    }

    @Override
    public MatrixEntry get(int i, ReusableMatrixEntry reusable) {
        return reusable.set(this.row, this.currentIndex, this.getValue(i));
    }

    @Override
    public MatrixEntry next() {
        return get(this.currentIndex);
    }

    @Override
    public MatrixEntry next(int i, ReusableMatrixEntry reusable) {
        return get(this.currentIndex, reusable);
    }

    @Override
    public boolean hasNext() {
        return this.currentIndex < this.size();
    }

    @Override
    public MatrixRecord set(String[] record) {
        throw new UnsupportedOperationException();
    }

    @Override
    public MatrixRecord set(String[] record, int[] projection, long row) {
        this.record = record;
        this.projection = projection;
        this.row = row;
        this.currentIndex = 0;
        return this;
    }
}
