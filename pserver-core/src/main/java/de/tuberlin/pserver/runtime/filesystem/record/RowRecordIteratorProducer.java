package de.tuberlin.pserver.runtime.filesystem.record;

import org.apache.commons.csv.CSVRecord;

public class RowRecordIteratorProducer extends AbstractCSVRecordIteratorProducer {

    private RowRecord reusable = new RowRecord(null, null, 0);

    //@Override
    //protected IRecord csvRecordToIRecord(String[] record, long row) {
    //    return reusable.set(record, projection, row);
    //}

    @Override
    protected IRecord csvRecordToIRecord(CSVRecord csvRecord, long row) {
        return reusable.set(csvRecord, projection, row);
    }
}
