package de.tuberlin.pserver.app.filesystem.record;

import de.tuberlin.pserver.app.types.ImmutableMatrixEntry;
import de.tuberlin.pserver.app.types.MatrixEntry;
import de.tuberlin.pserver.app.types.ReusableMatrixEntry;
import de.tuberlin.pserver.math.stuff.Arrays;

import java.util.NoSuchElementException;

/**
 * Wraps a {@link org.apache.commons.csv.CSVRecord} instance for compatibility with the generic {@link IRecord} interface.
 */
public class RowColValRecord implements IRecord {

    private org.apache.commons.csv.CSVRecord delegate;

    private boolean isFetched = false;

    public RowColValRecord(org.apache.commons.csv.CSVRecord delegate) {
        this.delegate = delegate;
    }

    public static RowColValRecord wrap(org.apache.commons.csv.CSVRecord record, int[] projection, long row) {
        return new RowColValRecord(record);
    }

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
            int row = Integer.parseInt(delegate.get(0));
            int col = Integer.parseInt(delegate.get(1));
            double val = Double.parseDouble(delegate.get(2));
            if(resuable == null) {
                return new ImmutableMatrixEntry(row, col, val);
            }
            return resuable.set(row, col, val);
        }
        catch(NullPointerException | NumberFormatException | IndexOutOfBoundsException e) {
            throw new RuntimeException("Record not parsable: " + delegate.toString(), e);
        }
    }

    @Override
    public RowColValRecord set(org.apache.commons.csv.CSVRecord delegate, int[] projection, long row) {
        isFetched = false;
        this.delegate = delegate;
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
