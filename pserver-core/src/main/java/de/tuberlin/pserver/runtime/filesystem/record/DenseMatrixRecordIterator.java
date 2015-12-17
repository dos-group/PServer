package de.tuberlin.pserver.runtime.filesystem.record;

import java.io.InputStream;

/**
 * Created by hegemon on 16.12.15.
 */
public class DenseMatrixRecordIterator extends MatrixRecordIterator {

    protected DenseMatrixRecordIterator(InputStream inputStream, char[] separator, char delimiter, int[] projection) {
        super(inputStream, separator, delimiter, projection);
        this.reusable = new DenseMatrixRecord(null, null, 0);
    }

    @Override
    protected MatrixRecord csvRecordToIRecord(String[] record, long row) {
        return this.reusable.set(record, this.projection, row);
    }

}
