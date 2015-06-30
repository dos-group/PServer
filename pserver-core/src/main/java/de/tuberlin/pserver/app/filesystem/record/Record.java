package de.tuberlin.pserver.app.filesystem.record;

/**
 * Wraps a {@link org.apache.commons.csv.CSVRecord} instance for compatibility with the generic {@link IRecord} interface.
 */
public class Record implements IRecord {

    private int[] projection;
    private int currentIndex = 0;

    private org.apache.commons.csv.CSVRecord delegate;

    public Record(org.apache.commons.csv.CSVRecord delegate, int[] projection) {
        this.delegate = delegate;
        this.projection = projection;
    }

    public static Record wrap(org.apache.commons.csv.CSVRecord record, int[] projection) {
        return new Record(record, projection);
    }

    @Override
    public int size() {
        return projection != null ? projection.length : delegate.size();
    }

    @Override
    public double get(int i) {
        if(projection != null) {
            i = projection[currentIndex++];
        }
        double result = Double.NaN;
        String value = delegate.get(i);
        try {
            result = Double.parseDouble(value);
        }
        catch(NumberFormatException e) {}
        return result;
    }

    public Record set(org.apache.commons.csv.CSVRecord record, int[] projection) {
        this.delegate = delegate;
        this.projection = projection;
        return this;
    }
}
