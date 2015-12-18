package de.tuberlin.pserver.runtime.filesystem.record;

import de.tuberlin.pserver.runtime.filesystem.Format;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;

/**
 * Created by Morgan K. Geldenhuys on 16.12.15.
 */
public interface RecordIterator<T extends Record> {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    Logger LOG = LoggerFactory.getLogger(RecordIterator.class);

    char DEFAULT_RECORD_SEPARATOR       = '\n';
    char DEFAULT_DELIMITER              = ',';
    int[] DEFAULT_PROJECTION            = null;

    // ---------------------------------------------------
    // Factory Methods.
    // ---------------------------------------------------

    static RecordIterator create(Format format, InputStream inputStream) {
        return RecordIterator.create(format, inputStream, DEFAULT_RECORD_SEPARATOR, DEFAULT_DELIMITER, DEFAULT_PROJECTION);
    }

    static RecordIterator create(Format format, InputStream inputStream, char separator, char delimiter, int[] projection) {
        switch(format) {
            case SPARSE_FORMAT: return new SparseMatrixRecordIterator(inputStream, separator, delimiter, projection);
            case DENSE_FORMAT: return new DenseMatrixRecordIterator(inputStream, separator, delimiter, projection);
            default: throw new IllegalArgumentException();
        }
    }

    // ---------------------------------------------------
    // Abstract Methods.
    // ---------------------------------------------------

    boolean hasNext();

    T next(long lineNumber);

}
