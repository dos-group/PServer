package de.tuberlin.pserver.core.filesystem.local;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.core.filesystem.FileDataIterator;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
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

        public CSVFileDataIterator(final LocalCSVFileSection csvFileSection) {
            try {

                this.csvFileSection = Preconditions.checkNotNull(csvFileSection);
                this.fileReader     = new FileReader(Preconditions.checkNotNull(fileName));
                this.csvFileParser  = new CSVParser(fileReader, format);
                this.csvIterator    = csvFileParser.iterator();

            } catch(Exception e) {
                close();
                throw new IllegalStateException(e);
            }
        }

        @Override
        public void initialize() {
            try {
                Preconditions.checkState(csvFileSection != null);
                this.fileReader.skip(csvFileSection.startOffset);
            } catch(Exception e) {
                close();
                throw new IllegalStateException(e);
            }
        }

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

    private final String fileName;

    private final CSVFormat format;

    private final LocalCSVFileSection csvFileSection;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public LocalCSVInputFile(final String fileName) {
        this(fileName, "\n", ',');
    }

    public LocalCSVInputFile(final String fileName, final String recordSeparator, final char delimiter) {
        Preconditions.checkNotNull(fileName);
        Preconditions.checkNotNull(recordSeparator);
        Preconditions.checkNotNull(delimiter);
        this.fileName   = fileName;
        this.format     = CSVFormat.DEFAULT.withRecordSeparator(recordSeparator).withDelimiter(delimiter);

        this.csvFileSection = new LocalCSVFileSection();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void computeLocalFileSection(final int numNodes, final int nodeIdx) {
        final long totalLines = getNumberOfLines();
        final long numLinesPerSection = totalLines / numNodes;
        final long linesToRead = nodeIdx == numNodes - 1 ?
                numLinesPerSection + (totalLines % numLinesPerSection) : numLinesPerSection;
        try {
            final long blockLineOffset = numLinesPerSection * nodeIdx;
            final BufferedReader br = new BufferedReader(new FileReader(fileName));
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
            final LineNumberReader lnr = new LineNumberReader(new FileReader(fileName));
            lnr.skip(Long.MAX_VALUE);
            final int numLines = lnr.getLineNumber();
            lnr.close();
            return  numLines;
        } catch(IOException ioe) {
            throw new IllegalStateException(ioe);
        }
    }
}
