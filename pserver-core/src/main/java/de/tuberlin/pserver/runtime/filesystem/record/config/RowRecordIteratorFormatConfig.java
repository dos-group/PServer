package de.tuberlin.pserver.runtime.filesystem.record.config;

import de.tuberlin.pserver.runtime.filesystem.record.IRecord;
import de.tuberlin.pserver.runtime.filesystem.record.RowRecord;
import org.apache.commons.csv.CSVRecord;


public class RowRecordIteratorFormatConfig extends AbstractCSVRecordIteratorProducer {

    private RowRecord reusable = new RowRecord(null, null, 0);

    @Override
    protected IRecord csvRecordToIRecord(CSVRecord csvRecord, long row) {
        return reusable.set(csvRecord, projection, row);
    }
}
