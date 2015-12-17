package de.tuberlin.pserver.runtime.filesystem.record;


/*public class RowColValRecordIteratorProducer extends AbstractCSVRecordIteratorProducer {

    private RowColValRecord reusable = new RowColValRecord(null);

    @Override
    protected IRecord csvRecordToIRecord(String[] record, long row) {

        if (record == null)
            return null;
            //throw new IllegalStateException("record == null");

        return reusable.set(record, projection, row);
    }
}*/

import org.apache.commons.csv.CSVRecord;

public class RowColValRecordIteratorProducer extends AbstractCSVRecordIteratorProducer {

    private RowColValRecord reusable = new RowColValRecord(null);

    @Override
    protected IRecord csvRecordToIRecord(CSVRecord csvRecord, long row) {
        return reusable.set(csvRecord, projection, row);
    }
}
