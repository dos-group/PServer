package de.tuberlin.pserver.runtime.filesystem.record;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Iterator;

/**
 * Created by Morgan K. Geldenhuys on 16.12.15.
 */
public interface RecordIterator<T> {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    Logger LOG = LoggerFactory.getLogger(RecordIterator.class);

    char[] DEFAULT_RECORD_SEPARATOR     = {'\n'};
    char DEFAULT_DELIMITER              = ',';
    int[] DEFAULT_PROJECTION            = null;

    // ---------------------------------------------------
    // Factory Methods.
    // ---------------------------------------------------

    static RecordIterator create(RecordType type, InputStream inputStream) {
        return RecordIterator.create(type, inputStream, DEFAULT_RECORD_SEPARATOR, DEFAULT_DELIMITER, DEFAULT_PROJECTION);
    }

    static RecordIterator create(RecordType type, InputStream inputStream, char[] separator, char delimiter, int[] projection) {
        switch(type) {
            case SPARSE: return new SparseMatrixRecordIterator(inputStream, separator, delimiter, projection);
            case DENSE: return new DenseMatrixRecordIterator(inputStream, separator, delimiter, projection);
            default: throw new IllegalArgumentException();
        }
    }

    // ---------------------------------------------------
    // Abstract Methods.
    // ---------------------------------------------------

    boolean hasNext();

    T next(long lineNumber);

}
