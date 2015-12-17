package de.tuberlin.pserver.runtime.filesystem.record;

import java.io.InputStream;

/**
 * Created by Morgan K. Geldenhuys on 16.12.15.
 */
public class SparseMatrixRecordIterator extends MatrixRecordIterator {

    protected SparseMatrixRecordIterator(InputStream inputStream, char[] separator, char delimiter, int[] projection) {
        super(inputStream, separator, delimiter, projection);
        this.reusable = new SparseMatrixRecord(null);
    }

    @Override
    protected MatrixRecord csvRecordToIRecord(String[] record, long row) {
        if (record == null)
            throw new IllegalStateException("record == null");

        return reusable.set(record, this.projection, row);
    }

}
