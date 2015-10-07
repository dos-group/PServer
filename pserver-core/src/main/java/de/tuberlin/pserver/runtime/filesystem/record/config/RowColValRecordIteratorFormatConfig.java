package de.tuberlin.pserver.runtime.filesystem.record.config;

import de.tuberlin.pserver.runtime.filesystem.record.IRecord;
import de.tuberlin.pserver.runtime.filesystem.record.RowColValRecord;
import org.apache.commons.csv.CSVRecord;


public class RowColValRecordIteratorFormatConfig extends AbstractCSVRecordIteratorProducer {

    private RowColValRecord reusable = new RowColValRecord(null);

    @Override
    protected IRecord csvRecordToIRecord(CSVRecord csvRecord, long row) {
        return reusable.set(csvRecord, projection, row);
    }
}
