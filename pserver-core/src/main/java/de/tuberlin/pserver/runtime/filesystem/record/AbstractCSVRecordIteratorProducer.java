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


public abstract class AbstractCSVRecordIteratorProducer implements IRecordIteratorProducer {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public static final char DEFAULT_RECORD_SEPARATOR  = '\n';
    public static final char DEFAULT_DELIMITER         = ',';
    // null value here means: do not project, take it all
    public static final int[] DEFAULT_PROJECTION       = null;

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    // TO BE DEFINED BY SUB CLASSES!

    //protected final CSVFormat csvFormat;

    protected final CsvParserSettings settings;

    protected final int[] projection;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public AbstractCSVRecordIteratorProducer(int[] projection, char delimiter, char recordSeparator) {
        this.projection = projection;
        //this.csvFormat = CSVFormat.DEFAULT.withDelimiter(delimiter).withRecordSeparator(recordSeparator);

        this.settings = new CsvParserSettings();
        //the file used in the example uses '\n' as the line separator sequence.
        //the line separator sequence is defined here to ensure systems such as MacOS and Windows
        //are able to process this file correctly (MacOS uses '\r'; and Windows uses '\r\n').
        settings.getFormat().setLineSeparator("\n");
    }

    public AbstractCSVRecordIteratorProducer() {
        this(DEFAULT_PROJECTION, DEFAULT_DELIMITER, DEFAULT_RECORD_SEPARATOR);
    }

    //protected abstract IRecord csvRecordToIRecord(CSVRecord csvRecord, long row);

    protected abstract IRecord csvRecordToIRecord(final String[] rowRecord, final long row);

    @Override
    public IRecordIterator getRecordIterator(InputStream inputStream) {
        return new CSVRecordIterator(inputStream);
    }

    private class CSVRecordIterator implements IRecordIterator {

        //protected final Iterator<CSVRecord> csvIterator;

        protected final CsvParser parser;

        private String[] parsedRow;

        public CSVRecordIterator(InputStream inputStream) {
            Preconditions.checkNotNull(inputStream);
            try {
                //csvIterator = new CSVParser(new InputStreamReader(inputStream), csvFormat).iterator();

                this.parser = new CsvParser(settings);

                parser.beginParsing(new InputStreamReader(inputStream));

            } catch (Exception e) {
                throw new IllegalStateException("Could not instantiate CSVParser", e);
            }
        }

        @Override
        public boolean hasNext() {
            parsedRow = parser.parseNext();
            return parsedRow != null;
        }

        @Override
        public IRecord next(long lineNumber) {
            return csvRecordToIRecord(parsedRow, lineNumber);
        }
    }
}
