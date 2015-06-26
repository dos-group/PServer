package de.tuberlin.pserver.app.filesystem.record;

/**
 * Wraps a {@link org.apache.commons.csv.CSVRecord} instance for compatibility with the generic {@link IRecord} interface.
 */
public class CSVRecordWrapper implements IRecord {

    private final org.apache.commons.csv.CSVRecord delegate;

    public CSVRecordWrapper(org.apache.commons.csv.CSVRecord delegate) {
        this.delegate = delegate;
    }

    public static CSVRecordWrapper wrap(org.apache.commons.csv.CSVRecord record) {
        return new CSVRecordWrapper(record);
    }

    @Override
    public int size() {
        return delegate.size();
    }

    @Override
    public double get(int i) {
        return Double.parseDouble(delegate.get(i));
    }
}
