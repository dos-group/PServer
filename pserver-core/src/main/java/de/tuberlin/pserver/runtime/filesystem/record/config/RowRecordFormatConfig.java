package de.tuberlin.pserver.runtime.filesystem.record.config;

import de.tuberlin.pserver.runtime.filesystem.record.IRecordFactory;

/**
 * Created by fsander on 26.08.15.
 */
public class RowRecordFormatConfig extends AbstractRecordFormatConfig {

    public RowRecordFormatConfig() {
        super();
        setRecordFactory(IRecordFactory.ROW_RECORD);
    }

}
