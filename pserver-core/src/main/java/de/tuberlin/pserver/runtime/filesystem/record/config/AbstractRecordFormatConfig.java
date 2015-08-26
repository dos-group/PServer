package de.tuberlin.pserver.runtime.filesystem.record.config;

import de.tuberlin.pserver.runtime.filesystem.record.IRecordFactory;
import org.apache.commons.csv.CSVFormat;


public class AbstractRecordFormatConfig {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public static final char DEFAULT_RECORD_SEPARATOR  = '\n';
    public static final char DEFAULT_DELIMITER         = ',';
    // null value here means: do not project, take it all
    public static final int[] DEFAULT_PROJECTION       = null;

    public static final AbstractRecordFormatConfig DEFAULT = new AbstractRecordFormatConfig(DEFAULT_PROJECTION, DEFAULT_DELIMITER, DEFAULT_RECORD_SEPARATOR);

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final CSVFormat csvFormat;

    private final int[] projection;

    private IRecordFactory recordFactory;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public AbstractRecordFormatConfig(int[] projection, char delimiter, char recordSeparator, IRecordFactory recordFactory) {
        this.projection = projection;
        this.csvFormat = CSVFormat.DEFAULT.withDelimiter(delimiter).withRecordSeparator(recordSeparator);
        this.recordFactory = recordFactory;
    }

    public AbstractRecordFormatConfig(int[] projection, char delimiter, char recordSeparator) {
        this(projection, delimiter, recordSeparator, IRecordFactory.ROW_RECORD);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public AbstractRecordFormatConfig(int[] projection, char delimiter) {
        this(projection, delimiter, DEFAULT_RECORD_SEPARATOR);
    }

    public AbstractRecordFormatConfig(char delimiter) {
        this(DEFAULT_PROJECTION, delimiter, DEFAULT_RECORD_SEPARATOR);
    }

    public AbstractRecordFormatConfig(int[] projection) {
        this(projection, DEFAULT_DELIMITER, DEFAULT_RECORD_SEPARATOR);
    }

    public AbstractRecordFormatConfig() {
        this(DEFAULT_PROJECTION, DEFAULT_DELIMITER, DEFAULT_RECORD_SEPARATOR);
    }

    public AbstractRecordFormatConfig setRecordFactory(IRecordFactory recordFactory) {
        this.recordFactory = recordFactory;
        return this;
    }

    public CSVFormat getCsvFormat() {
        return csvFormat;
    }

    public int[] getProjection() {
        return projection;
    }

    public IRecordFactory getRecordFactory() {
        return recordFactory;
    }
}
