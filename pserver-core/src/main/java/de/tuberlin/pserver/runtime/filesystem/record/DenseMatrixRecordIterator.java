package de.tuberlin.pserver.runtime.filesystem.record;

import org.apache.commons.csv.CSVRecord;

import java.io.InputStream;

/**
 * Created by Morgan K. Geldenhuys on 16.12.15.
 */
public class DenseMatrixRecordIterator extends MatrixRecordIterator {

    protected DenseMatrixRecordIterator(InputStream inputStream, char separator, char delimiter, int[] projection) {
        super(inputStream, separator, delimiter, projection);
        this.reusable = new DenseMatrixRecord(null, null, 0);
    }

    @Override
    protected MatrixRecord csvRecordToIRecord(CSVRecord csvRecord, long row) {
        return this.reusable.set(csvRecord, this.projection, row);
    }

}
