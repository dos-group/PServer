package de.tuberlin.pserver.runtime.filesystem.record;

import com.google.common.base.Preconditions;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;

/**
 * Created by Morgan K. Geldenhuys on 16.12.15.
 */

public abstract class MatrixRecordIterator implements RecordIterator<MatrixRecord> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected MatrixRecord reusable;
    protected InputStream inputStream;
    protected Iterator<CSVRecord> csvIterator;
    protected int[] projection;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    protected MatrixRecordIterator(InputStream inputStream, char separator, char delimiter, int[] projection) {

        CSVFormat csvFormat =
            CSVFormat.DEFAULT
                .withRecordSeparator(separator)
                .withDelimiter(delimiter);
        this.projection = projection;
        this.inputStream = Preconditions.checkNotNull(inputStream);

        try {
            this.csvIterator = new CSVParser(new InputStreamReader(inputStream), csvFormat).iterator();
        }
        catch (IOException e) {
            throw new IllegalStateException("Could not instantiate CSVParser", e);
        }
    }

    // ---------------------------------------------------
    // Abstract Methods.
    // ---------------------------------------------------

    abstract protected MatrixRecord csvRecordToIRecord(CSVRecord csvRecord, long row);

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public boolean hasNext() {
        return this.csvIterator.hasNext();
    }

    @Override
    public MatrixRecord next(long lineNumber) {
        return csvRecordToIRecord(this.csvIterator.next(), lineNumber);
    }

}
