package de.tuberlin.pserver.runtime.filesystem.record;

import de.tuberlin.pserver.runtime.filesystem.Format;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Iterator;

/**
 * Created by Morgan K. Geldenhuys on 16.12.15.
 */
public interface RecordIterator<T extends Record> extends Iterator<T> {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    Logger LOG = LoggerFactory.getLogger(RecordIterator.class);

    // ---------------------------------------------------
    // Factory Methods.
    // ---------------------------------------------------

    static RecordIterator create(Format format, InputStream inputStream) {
        switch(format) {
            case SPARSE_FORMAT: return new SparseMatrixRecordIterator(inputStream, '\n', ',', new int[]{});
            case DENSE_FORMAT:  return new DenseMatrixRecordIterator(inputStream, '\n', ',', new int[]{});
            case SVM_FORMAT:    return new SVMRecordIterator(inputStream, '\n', ' ', new int[]{});
            default:            throw new IllegalArgumentException();
        }
    }

    static RecordIterator create(Format format, InputStream inputStream, char separator, char delimiter, int[] projection) {
        switch(format) {
            case SPARSE_FORMAT: return new SparseMatrixRecordIterator(inputStream, separator, delimiter, projection);
            case DENSE_FORMAT:  return new DenseMatrixRecordIterator(inputStream, separator, delimiter, projection);
            case SVM_FORMAT:    return new SVMRecordIterator(inputStream, separator, delimiter, projection);
            default:            throw new IllegalArgumentException();
        }
    }

    // ---------------------------------------------------
    // Abstract Methods.
    // ---------------------------------------------------

    T next(long lineNumber);

}
