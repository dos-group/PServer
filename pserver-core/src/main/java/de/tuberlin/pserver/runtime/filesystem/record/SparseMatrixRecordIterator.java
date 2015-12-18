package de.tuberlin.pserver.runtime.filesystem.record;

import org.apache.commons.csv.CSVRecord;

import java.io.InputStream;

/**
 * Created by Morgan K. Geldenhuys on 16.12.15.
 */
public class SparseMatrixRecordIterator extends MatrixRecordIterator {

    protected SparseMatrixRecordIterator(InputStream inputStream, char separator, char delimiter, int[] projection) {
        super(inputStream, separator, delimiter, projection);
        this.reusable = new SparseMatrixRecord(null);
    }

    @Override
    protected MatrixRecord csvRecordToIRecord(CSVRecord csvRecord, long row) {
        return reusable.set(csvRecord, this.projection, row);
    }

}
