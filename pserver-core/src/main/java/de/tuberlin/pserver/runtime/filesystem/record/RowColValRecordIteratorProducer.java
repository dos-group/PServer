package de.tuberlin.pserver.runtime.filesystem.record;

import org.apache.commons.csv.CSVRecord;


public class RowColValRecordIteratorProducer extends AbstractCSVRecordIteratorProducer {

    private RowColValRecord reusable = new RowColValRecord(null);

    @Override
    protected IRecord csvRecordToIRecord(CSVRecord csvRecord, long row) {
        return reusable.set(csvRecord, projection, row);
    }
}
