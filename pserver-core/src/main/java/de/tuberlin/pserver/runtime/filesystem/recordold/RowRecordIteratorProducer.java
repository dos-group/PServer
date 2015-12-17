package de.tuberlin.pserver.runtime.filesystem.recordold;

public class RowRecordIteratorProducer extends AbstractCSVRecordIteratorProducer {

    private RowRecord reusable = new RowRecord(null, null, 0);

    @Override
    protected IRecord csvRecordToIRecord(String[] record, long row) {
        return reusable.set(record, projection, row);
    }
}
