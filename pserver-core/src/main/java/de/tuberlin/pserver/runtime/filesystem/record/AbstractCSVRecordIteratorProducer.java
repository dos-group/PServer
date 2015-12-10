package de.tuberlin.pserver.runtime.filesystem.record;

import com.google.common.base.Preconditions;
import com.univocity.parsers.common.input.CharInputReader;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;


public abstract class AbstractCSVRecordIteratorProducer implements IRecordIteratorProducer {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(AbstractCSVRecordIteratorProducer.class);


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

    protected abstract IRecord csvRecordToIRecord(final String[] rowRecord, final long row);

    @Override
    public IRecordIterator getRecordIterator(InputStream inputStream) {
        return new CSVRecordIterator(inputStream);
    }

    private class CSVRecordIterator implements IRecordIterator {

        protected final CsvParser parser;

        protected final InputStream inputStream;

        public CSVRecordIterator(InputStream inputStream) {
            try {

                this.inputStream = Preconditions.checkNotNull(inputStream);

                this.parser = new CsvParser(settings);

                parser.beginParsing(new InputStreamReader(inputStream));

            } catch (Exception e) {
                throw new IllegalStateException("Could not instantiate CSVParser", e);
            }
        }

        @Override
        public boolean hasNext() {
            try {
                return inputStream.available() != 0; // TODO: THIS SHOULD BE CHANGES! NOT A SAFE WAY!
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public IRecord next(long lineNumber) {
            return csvRecordToIRecord(parser.parseNext(), lineNumber);
        }
    }
}
