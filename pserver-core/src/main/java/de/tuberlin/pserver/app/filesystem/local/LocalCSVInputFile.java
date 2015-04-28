package de.tuberlin.pserver.app.filesystem.local;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.app.filesystem.FileDataIterator;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.Iterator;

public class LocalCSVInputFile implements LocalInputFile<CSVRecord> {

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    private class LocalCSVFileSection {

        public long totalLines  = 0;

        public long linesToRead = 0;

        public long startOffset = 0;

        public void set(final long totalLines, final long linesToRead, final long startOffset) {
            this.totalLines     = totalLines;
            this.linesToRead    = linesToRead;
            this.startOffset    = startOffset;
        }
    }

    // ---------------------------------------------------

    private class CSVFileDataIterator implements FileDataIterator<CSVRecord> {

        private final FileReader fileReader;

        private final CSVParser csvFileParser;

        private final Iterator<CSVRecord> csvIterator;

        private final LocalCSVFileSection csvFileSection;

        private long currentLine = 0;

        // ---------------------------------------------------

        public CSVFileDataIterator(final LocalCSVFileSection csvFileSection) {
            try {

                this.csvFileSection = Preconditions.checkNotNull(csvFileSection);
                this.fileReader     = new FileReader(Preconditions.checkNotNull(filePath));
                this.csvFileParser  = new CSVParser(fileReader, format);
                this.csvIterator    = csvFileParser.iterator();

            } catch(Exception e) {
                close();
                throw new IllegalStateException(e);
            }
        }

        // ---------------------------------------------------

        @Override
        public void initialize() {
            try {
                Preconditions.checkState(csvFileSection != null);
                fileReader.skip(csvFileSection.startOffset);
            } catch(Exception e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public void reset() { /*iterator = null;*/ }

        @Override
        public String getFilePath() { return filePath; }

        @Override
        public boolean hasNext() {
            final boolean hasNext = currentLine < csvFileSection.linesToRead && csvIterator.hasNext();
            if (!hasNext)
                close();
            return hasNext;
        }

        @Override
        public CSVRecord next() {
            final CSVRecord record = csvIterator.next();
            ++currentLine;
            return record;
        }

        // ---------------------------------------------------

        private void close() {
            try {
                if (csvFileParser != null)
                    csvFileParser.close();
                if (fileReader != null)
                    fileReader.close();
            } catch (IOException ioe) {
                throw new IllegalStateException(ioe);
            }
        }
    };

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(LocalCSVInputFile.class);

    private final String filePath;

    private final CSVFormat format;

    private final LocalCSVFileSection csvFileSection;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public LocalCSVInputFile(final String filePath) {
        this(filePath, "\n", ',');
    }

    public LocalCSVInputFile(final String filePath, final String recordSeparator, final char delimiter) {
        Preconditions.checkNotNull(filePath);
        Preconditions.checkNotNull(recordSeparator);
        Preconditions.checkNotNull(delimiter);
        this.filePath = filePath;
        this.format   = CSVFormat.DEFAULT.withRecordSeparator(recordSeparator).withDelimiter(delimiter);
        this.csvFileSection = new LocalCSVFileSection();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void computeLocalFileSection(final int numNodes, final int instanceID) {
        final long totalLines = getNumberOfLines();
        final long numLinesPerSection = totalLines / numNodes;
        final long linesToRead = (instanceID == (numNodes - 1)) ?
                numLinesPerSection + (totalLines % numLinesPerSection) : numLinesPerSection;
        try {
            final long blockLineOffset = numLinesPerSection * instanceID;
            final BufferedReader br = new BufferedReader(new FileReader(filePath));
            long startOffset = 0;
            for (int i = 0; i < totalLines; ++i) {
                if (i < blockLineOffset)
                    startOffset += br.readLine().length() + 1;
                else break;
            }
            br.close();
            csvFileSection.set(totalLines, linesToRead, startOffset);
        } catch (IOException ioe) {
            throw new IllegalStateException(ioe);
        }
    }

    @Override
    public FileDataIterator<CSVRecord> iterator() { return new CSVFileDataIterator(csvFileSection); }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private long getNumberOfLines() {
        try {
            final LineNumberReader lnr = new LineNumberReader(new FileReader(filePath));
            lnr.skip(Long.MAX_VALUE);
            final int numLines = lnr.getLineNumber();
            lnr.close();
            return  numLines;
        } catch(IOException ioe) {
            throw new IllegalStateException(ioe);
        }
    }
}
