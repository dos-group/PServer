package de.tuberlin.pserver.runtime.filesystem.record.config;

import de.tuberlin.pserver.runtime.filesystem.record.IRecordFactory;


public class RowRecordFormatConfig extends AbstractRecordFormatConfig {

    public RowRecordFormatConfig() {
        super();
        setRecordFactory(IRecordFactory.ROW_RECORD);
    }

}
