package de.tuberlin.pserver.runtime.filesystem.record;

import com.google.common.base.Preconditions;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Created by Morgan K. Geldenhuys on 16.12.15.
 */

public abstract class MatrixRecordIterator implements RecordIterator<MatrixRecord> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected MatrixRecord reusable;
    protected InputStream inputStream;
    protected CsvParserSettings settings;
    protected CsvParser parser;
    protected int[] projection;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    protected MatrixRecordIterator(InputStream inputStream, char[] separator, char delimiter, int[] projection) {
        this.settings = new CsvParserSettings();
        this.settings.getFormat().setLineSeparator(separator);
        this.settings.getFormat().setDelimiter(delimiter);
        this.projection = projection;

        try {
            this.inputStream = Preconditions.checkNotNull(inputStream);
            this.parser = new CsvParser(settings);
            parser.beginParsing(new InputStreamReader(inputStream));
        }
        catch (Exception e) {
            throw new IllegalStateException("Could not instantiate CSVParser", e);
        }
    }

    // ---------------------------------------------------
    // Abstract Methods.
    // ---------------------------------------------------

    abstract protected MatrixRecord csvRecordToIRecord(String[] record, long row);

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public boolean hasNext() {
        try {
            return inputStream.available() != 0; // TODO: THIS SHOULD BE CHANGED! NOT A SAFE WAY!
        }
        catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public MatrixRecord next(long lineNumber) {
        return this.csvRecordToIRecord(parser.parseNext(), lineNumber);
    }

}
