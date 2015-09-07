package de.tuberlin.pserver.runtime.filesystem.record.config;

import de.tuberlin.pserver.runtime.filesystem.record.IRecordFactory;


public class RowColValRecordFormatConfig extends AbstractRecordFormatConfig {

    public RowColValRecordFormatConfig() {
        super();
        setRecordFactory(IRecordFactory.ROWCOLVAL_RECORD);
    }

}
