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

    protected InputStream inputStream;
    private long currentLine;
    protected char separator;
    protected char delimiter;
    protected int[] projection;
    protected Iterator<CSVRecord> csvIterator;
    protected MatrixRecord reusable;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    protected MatrixRecordIterator(InputStream inputStream, char separator, char delimiter, int[] projection) {
        this.inputStream = Preconditions.checkNotNull(inputStream);
        this.currentLine = 1;
        this.separator = separator;
        this.delimiter = delimiter;
        this.projection = projection;

        CSVFormat csvFormat =
            CSVFormat.DEFAULT
                .withRecordSeparator(this.separator)
                .withDelimiter(this.delimiter);

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

    abstract protected MatrixRecord csvRecordToRecord(CSVRecord csvRecord, long row);

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public boolean hasNext() {
        return this.csvIterator.hasNext();
    }

    @Override
    public MatrixRecord next() {
        return this.csvRecordToRecord(this.csvIterator.next(), this.currentLine++);
    }

    @Override
    public MatrixRecord next(long lineNumber) {
        this.currentLine = lineNumber;
        return this.csvRecordToRecord(this.csvIterator.next(), lineNumber);
    }

}
