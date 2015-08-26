package de.tuberlin.pserver.runtime.filesystem.record.config;

import de.tuberlin.pserver.runtime.filesystem.record.IRecordFactory;

/**
 * Created by fsander on 26.08.15.
 */
public class RowColValRecordFormatConfig extends AbstractRecordFormatConfig {

    public RowColValRecordFormatConfig() {
        super();
        setRecordFactory(IRecordFactory.ROWCOLVAL_RECORD);
    }

}
