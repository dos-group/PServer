package de.tuberlin.pserver.app.filesystem.record;

import de.tuberlin.pserver.app.types.ImmutableMatrixEntry;
import de.tuberlin.pserver.app.types.MatrixEntry;
import de.tuberlin.pserver.app.types.ReusableMatrixEntry;
import org.apache.commons.csv.CSVRecord;

/**
 * Wraps a {@link org.apache.commons.csv.CSVRecord} instance for compatibility with the generic {@link IRecord} interface.
 */
public class RowRecord implements IRecord {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private int[] projection;

    private int currentIndex = 0;

    private long row;

    private org.apache.commons.csv.CSVRecord delegate;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public RowRecord(CSVRecord delegate, int[] projection, long row) {
        this.delegate = delegate;
        this.projection = projection;
        this.row = row;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public int size() {
        return projection != null ? projection.length : delegate.size();
    }

    private double getValue(int i) {
        if(projection != null) {
            i = projection[currentIndex];
        }
        double result = Double.NaN;
        String value = delegate.get(i);
        try {
            result = Double.parseDouble(value);
        }
        catch(NumberFormatException e) {}
        currentIndex++;
        return result;
    }

    @Override
    public MatrixEntry get(int i) {
        return new ImmutableMatrixEntry(row, currentIndex, getValue(i));
    }

    public MatrixEntry get(int i, ReusableMatrixEntry resuable) {
        return resuable.set(row, currentIndex, getValue(i));
    }

    @Override
    public RowRecord set(org.apache.commons.csv.CSVRecord delegate, int[] projection, long row) {
        this.delegate = delegate;
        this.projection = projection;
        this.row = row;
        currentIndex = 0;
        return this;
    }

    @Override
    public boolean hasNext() {
        return currentIndex < size();
    }

    @Override
    public MatrixEntry next() {
        return get(currentIndex);
    }

    @Override
    public MatrixEntry next(ReusableMatrixEntry reusable) {
        return get(currentIndex, reusable);
    }
}
