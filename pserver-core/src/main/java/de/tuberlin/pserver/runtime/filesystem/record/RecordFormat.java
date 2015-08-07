package de.tuberlin.pserver.runtime.filesystem.record;

import org.apache.commons.csv.CSVFormat;


public class RecordFormat {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public static final char DEFAULT_RECORD_SEPARATOR  = '\n';
    public static final char DEFAULT_DELIMITER         = ',';
    // null value here means: do not project, take it all
    public static final int[] DEFAULT_PROJECTION       = null;

    public static final RecordFormat DEFAULT = new RecordFormat(DEFAULT_PROJECTION, DEFAULT_DELIMITER, DEFAULT_RECORD_SEPARATOR);

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final CSVFormat csvFormat;

    private final int[] projection;

    private IRecordFactory recordFactory;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public RecordFormat(int[] projection, char delimiter, char recordSeparator, IRecordFactory recordFactory) {
        this.projection = projection;
        this.csvFormat = CSVFormat.DEFAULT.withDelimiter(delimiter).withRecordSeparator(recordSeparator);
        this.recordFactory = recordFactory;
    }

    public RecordFormat(int[] projection, char delimiter, char recordSeparator) {
        this(projection, delimiter, recordSeparator, IRecordFactory.ROW_RECORD);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public RecordFormat(int[] projection, char delimiter) {
        this(projection, delimiter, DEFAULT_RECORD_SEPARATOR);
    }

    public RecordFormat(char delimiter) {
        this(DEFAULT_PROJECTION, delimiter, DEFAULT_RECORD_SEPARATOR);
    }

    public RecordFormat(int[] projection) {
        this(projection, DEFAULT_DELIMITER, DEFAULT_RECORD_SEPARATOR);
    }

    public RecordFormat() {
        this(DEFAULT_PROJECTION, DEFAULT_DELIMITER, DEFAULT_RECORD_SEPARATOR);
    }

    public RecordFormat setRecordFactory(IRecordFactory recordFactory) {
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
