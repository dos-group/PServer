package de.tuberlin.pserver.app.filesystem.record;

import org.apache.commons.csv.CSVRecord;

public interface IRecordFactory {

    // TODO: this signature sucks. Generalize
    public IRecord wrap(CSVRecord record, int[] projection, long row);

    public static final IRecordFactory ROW_RECORD = new IRecordFactory() {
        @Override
        public IRecord wrap(CSVRecord record, int[] projection, long row) {
            return new RowRecord(record, projection, row);
        }
    };

    public static final IRecordFactory ROWCOLVAL_RECORD = new IRecordFactory() {
        @Override
        public IRecord wrap(CSVRecord record, int[] projection, long row) {
            return new RowColValRecord(record);
        }
    };

}
