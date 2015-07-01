package de.tuberlin.pserver.app.filesystem.record;

import de.tuberlin.pserver.app.types.ImmutableMatrixEntry;
import de.tuberlin.pserver.app.types.MatrixEntry;
import de.tuberlin.pserver.app.types.ReusableMatrixEntry;

/**
 * Wraps a {@link org.apache.commons.csv.CSVRecord} instance for compatibility with the generic {@link IRecord} interface.
 */
public class Record implements IRecord {

    private int[] projection;
    private int currentIndex = 0;
    private long row;

    private org.apache.commons.csv.CSVRecord delegate;

    public Record(org.apache.commons.csv.CSVRecord delegate, int[] projection, long row) {
        this.delegate = delegate;
        this.projection = projection;
        this.row = row;
    }

    public static Record wrap(org.apache.commons.csv.CSVRecord record, int[] projection, long row) {
        return new Record(record, projection, row);
    }

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
        return new ImmutableMatrixEntry(-1, currentIndex, getValue(i));
    }

    public MatrixEntry get(int i, ReusableMatrixEntry resuable) {
        return resuable.set(row, currentIndex, getValue(i));
    }

    public Record set(org.apache.commons.csv.CSVRecord delegate, int[] projection, long row) {
        this.delegate = delegate;
        this.projection = projection;
        this.row = row;
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
