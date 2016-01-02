package de.tuberlin.pserver.runtime.filesystem.record;

import de.tuberlin.pserver.runtime.state.entries.ImmutableMatrixEntry;
import de.tuberlin.pserver.runtime.state.entries.MatrixEntry;
import de.tuberlin.pserver.runtime.state.entries.ReusableMatrixEntry;
import org.apache.commons.csv.CSVRecord;

import java.util.NoSuchElementException;

/**
 * Created by Morgan K. Geldenhuys on 15.12.15.
 */
public class SparseMatrixRecord extends MatrixRecord<MatrixEntry, ReusableMatrixEntry> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private boolean isFetched;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public SparseMatrixRecord(CSVRecord delegate) {
        super(delegate);
        this.isFetched = false;
    }

    // ---------------------------------------------------
    // Static Methods.
    // ---------------------------------------------------

    public static SparseMatrixRecord wrap(CSVRecord csvRecord, int[] projection, long row) {
        return new SparseMatrixRecord(csvRecord);
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
        return this.get(i, null);
    }

    @Override
    public MatrixEntry get(int i, ReusableMatrixEntry reusable) {
        if (this.isFetched)
            throw new NoSuchElementException();

        this.isFetched = true;
        try {
            int row = Integer.parseInt(delegate.get(0));
            int col = Integer.parseInt(delegate.get(1));
            double val = Double.parseDouble(delegate.get(2));
            if (reusable == null)
                return new ImmutableMatrixEntry(row, col, val);

            return (MatrixEntry) reusable.set(row, col, val);
        }
        catch(NullPointerException | NumberFormatException | IndexOutOfBoundsException e) {
            throw new RuntimeException("Record not parsable: " + this.delegate.toString(), e);
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
    public MatrixEntry next(ReusableMatrixEntry reusable) {
        return get(0, reusable);
    }

    @Override
    public MatrixRecord set(CSVRecord csvRecord) {
        this.delegate = csvRecord;
        this.isFetched = false;
        return this;
    }

    @Override
    public MatrixRecord set(CSVRecord csvRecord, int[] projection, long row) {
        throw new UnsupportedOperationException("Use alternative set method");
    }

}
