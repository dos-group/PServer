package de.tuberlin.pserver.app.filesystem.record;

import org.apache.commons.csv.CSVFormat;

/**
 * Created by fsander on 29.06.15.
 */
public class RecordFormat {

    public static final char DEFAULT_RECORD_SEPARATOR  = '\n';
    public static final char DEFAULT_DELIMITER         = ',';
    // null value here means: do not project, take it all
    public static final int[] DEFAULT_PROJECTION       = null;

    public static final RecordFormat DEFAULT = new RecordFormat(DEFAULT_PROJECTION, DEFAULT_DELIMITER, DEFAULT_RECORD_SEPARATOR);

    private final CSVFormat csvFormat;

    private final int[] projection;

    public RecordFormat(int[] projection, char delimiter, char recordSeparator) {
        this.projection = projection;
        this.csvFormat = CSVFormat.DEFAULT.withDelimiter(delimiter).withRecordSeparator(recordSeparator);
    }

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

    public CSVFormat getCsvFormat() {
        return csvFormat;
    }

    public int[] getProjection() {
        return projection;
    }
}
