package de.tuberlin.pserver.runtime.filesystem.record;


public class RowColValRecordIteratorProducer extends AbstractCSVRecordIteratorProducer {

    private RowColValRecord reusable = new RowColValRecord(null);

    @Override
    protected IRecord csvRecordToIRecord(String[] record, long row) {

        if (record == null)
            throw new IllegalStateException("record == null");

        return reusable.set(record, projection, row);
    }
}
