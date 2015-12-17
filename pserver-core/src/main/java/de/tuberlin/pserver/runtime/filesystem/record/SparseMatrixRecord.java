package de.tuberlin.pserver.runtime.filesystem.record;

import de.tuberlin.pserver.runtime.state.mtxentries.ImmutableMatrixEntry;
import de.tuberlin.pserver.runtime.state.mtxentries.MatrixEntry;
import de.tuberlin.pserver.runtime.state.mtxentries.ReusableMatrixEntry;

import java.util.NoSuchElementException;

/**
 * Created by Morgan K. Geldenhuys on 15.12.15.
 */
public class SparseMatrixRecord extends MatrixRecord {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private boolean isFetched;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public SparseMatrixRecord(String[] record) {
        super(record);
        this.isFetched = false;
    }

    // ---------------------------------------------------
    // Static Methods.
    // ---------------------------------------------------

    public static SparseMatrixRecord wrap(String[] record, int[] projection, long row) {
        return new SparseMatrixRecord(record);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public int size() {
        return 1;
    }

    @Override
    public MatrixEntry get(int i) {
        return get(i, null);
    }

    @Override
    public MatrixEntry get(int i, ReusableMatrixEntry reusable) {
        if (this.isFetched)
            throw new NoSuchElementException();

        this.isFetched = true;
        try {
            int row = Integer.parseInt(this.record[0]);
            int col = Integer.parseInt(this.record[1]);
            double val = Double.parseDouble(this.record[2]);
            if (reusable == null)
                return new ImmutableMatrixEntry(row, col, val);

            return reusable.set(row, col, val);
        }
        catch(NullPointerException | NumberFormatException | IndexOutOfBoundsException e) {
            throw new RuntimeException("Record not parsable: " + this.record, e);
        }
    }

    @Override
    public boolean hasNext() {
        return this.isFetched;
    }

    @Override
    public MatrixEntry next() {
        return get(0, null);
    }

    @Override
    public MatrixEntry next(int i, ReusableMatrixEntry reusable) {
        return get(0, reusable);
    }

    @Override
    public MatrixRecord set(String[] record) {
        this.record = record;
        this.isFetched = false;
        return this;
    }

    @Override
    public MatrixRecord set(String[] record, int[] projection, long row) {
        throw new UnsupportedOperationException("Use alternative set method");
    }

}
