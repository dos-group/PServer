package de.tuberlin.pserver.app.filesystem.record;

import org.apache.commons.csv.CSVRecord;

public interface IRecordFactory {

    // TODO: this signature sucks. Generalize
    public IRecord wrap(CSVRecord record, int[] projection, long row);

    public static final IRecordFactory ROW_RECORD = new IRecordFactory() {

        private RowRecord reusable = new RowRecord(null, null, 0);

        @Override
        public IRecord wrap(CSVRecord record, int[] projection, long row) {
            return reusable.set(record, projection, row);
        }
    };

    public static final IRecordFactory ROWCOLVAL_RECORD = new IRecordFactory() {

        private RowColValRecord reusable = new RowColValRecord(null);

        @Override
        public IRecord wrap(CSVRecord record, int[] projection, long row) {
            return reusable.set(record, projection, row);
        }
    };

}
