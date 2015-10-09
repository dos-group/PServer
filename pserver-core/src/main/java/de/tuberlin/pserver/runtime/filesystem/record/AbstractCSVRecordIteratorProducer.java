package de.tuberlin.pserver.runtime.filesystem.record;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.runtime.filesystem.record.IRecord;
import de.tuberlin.pserver.runtime.filesystem.record.IRecordIterator;
import de.tuberlin.pserver.runtime.filesystem.record.IRecordIteratorProducer;
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

    protected final CSVFormat csvFormat;

    protected final int[] projection;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public AbstractCSVRecordIteratorProducer(int[] projection, char delimiter, char recordSeparator) {
        this.projection = projection;
        this.csvFormat = CSVFormat.DEFAULT.withDelimiter(delimiter).withRecordSeparator(recordSeparator);
    }

    public AbstractCSVRecordIteratorProducer() {
        this(DEFAULT_PROJECTION, DEFAULT_DELIMITER, DEFAULT_RECORD_SEPARATOR);
    }

    protected abstract IRecord csvRecordToIRecord(CSVRecord csvRecord, long row);

    @Override
    public IRecordIterator getRecordIterator(InputStream inputStream) {
        return new CSVRecordIterator(inputStream);
    }

    private class CSVRecordIterator implements IRecordIterator {

        protected final Iterator<CSVRecord> csvIterator;

        public CSVRecordIterator(InputStream inputStream) {
            Preconditions.checkNotNull(inputStream);
            try {
                csvIterator = new CSVParser(new InputStreamReader(inputStream), csvFormat).iterator();
            } catch (IOException e) {
                throw new IllegalStateException("Could not instantiate CSVParser", e);
            }
        }

        @Override
        public boolean hasNext() {
            return csvIterator.hasNext();
        }

        @Override
        public IRecord next(long lineNumber) {
            return csvRecordToIRecord(csvIterator.next(), lineNumber);
        }
    }


}
