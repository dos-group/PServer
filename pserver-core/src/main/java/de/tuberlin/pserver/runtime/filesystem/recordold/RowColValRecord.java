package de.tuberlin.pserver.runtime.filesystem.recordold;

import de.tuberlin.pserver.runtime.state.mtxentries.ImmutableMatrixEntry;
import de.tuberlin.pserver.runtime.state.mtxentries.MatrixEntry;
import de.tuberlin.pserver.runtime.state.mtxentries.ReusableMatrixEntry;

import java.util.NoSuchElementException;

/**
 * Wraps a {@link org.apache.commons.csv.CSVRecord} instance for compatibility with the generic {@link IRecord} interface.
 */
public class RowColValRecord implements IRecord {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private String[] record;

    private boolean isFetched = false;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public static RowColValRecord wrap(String[] record, int[] projection, long row) {
        return new RowColValRecord(record);
    }

    public RowColValRecord(String[] record) {
        this.record = record;
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

    public MatrixEntry get(int i, ReusableMatrixEntry resuable) {
        if(isFetched) {
            throw new NoSuchElementException();
        }
        isFetched = true;
        try {
            int row = Integer.parseInt(record[0]);
            int col = Integer.parseInt(record[1]);
            double val = Double.parseDouble(record[2]);
            if(resuable == null) {
                return new ImmutableMatrixEntry(row, col, val);
            }
            return resuable.set(row, col, val);
        }
        catch(NullPointerException | NumberFormatException | IndexOutOfBoundsException e) {
            throw new RuntimeException("Record not parsable: " + record, e);
        }
    }

    @Override
    public RowColValRecord set(String[] record, int[] projection, long row) {
        isFetched = false;
        this.record = record;
        return this;
    }

    @Override
    public boolean hasNext() {
        return !isFetched;
    }

    @Override
    public MatrixEntry next() {
        return get(0, null);
    }

    @Override
    public MatrixEntry next(ReusableMatrixEntry reusable) {
        return get(0, reusable);
    }
}
