package de.tuberlin.pserver.app.filesystem.local;

import org.apache.commons.csv.CSVFormat;

/**
 * Created by fsander on 29.06.15.
 */
public class InputFormat {

    public static final char DEFAULT_RECORD_SEPARATOR  = '\n';
    public static final char DEFAULT_DELIMITER         = ',';
    // null value here means: do not project, take it all
    public static final int[] DEFAULT_PROJECTION       = null;

    public static final InputFormat DEFAULT = new InputFormat(DEFAULT_PROJECTION, DEFAULT_DELIMITER, DEFAULT_RECORD_SEPARATOR);

    private final CSVFormat csvFormat;

    private final int[] projection;

    public InputFormat(int[] projection, char delimiter, char recordSeparator) {
        this.projection = projection;
        this.csvFormat = CSVFormat.DEFAULT.withDelimiter(delimiter).withRecordSeparator(recordSeparator);
    }

    public InputFormat(int[] projection, char delimiter) {
        this(projection, delimiter, DEFAULT_RECORD_SEPARATOR);
    }

    public InputFormat(char delimiter) {
        this(DEFAULT_PROJECTION, delimiter, DEFAULT_RECORD_SEPARATOR);
    }

    public InputFormat(int[] projection) {
        this(projection, DEFAULT_DELIMITER, DEFAULT_RECORD_SEPARATOR);
    }

    public InputFormat() {
        this(DEFAULT_PROJECTION, DEFAULT_DELIMITER, DEFAULT_RECORD_SEPARATOR);
    }

    public CSVFormat getCsvFormat() {
        return csvFormat;
    }

    public int[] getProjection() {
        return projection;
    }
}
