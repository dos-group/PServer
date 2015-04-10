package de.tuberlin.pserver.core.filesystem.local;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.core.filesystem.FileDataIterator;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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
        private final List<CSVRecord> records;
        private Iterator<CSVRecord> iterator = null;

        public CSVFileDataIterator(final LocalCSVFileSection csvFileSection) {
            try {

                this.csvFileSection = Preconditions.checkNotNull(csvFileSection);
                this.fileReader     = new FileReader(Preconditions.checkNotNull(fileName));
                this.csvFileParser  = new CSVParser(fileReader, format);
                this.csvIterator    = csvFileParser.iterator();
                this.records        = new ArrayList<>();

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

                while (currentLine < csvFileSection.linesToRead && csvIterator.hasNext()) {
                    records.add(csvIterator.next());
                    ++currentLine;
                }

            } catch(Exception e) {
                throw new IllegalStateException(e);
            } finally {
                close();
            }
        }

        @Override
        public void reset() {
            iterator = null;
        }

        @Override
        public boolean hasNext() {
            //if (currentLine == 0)
            //    LOG.info("START AT: " + csvFileSection.startOffset);
            //final boolean hasNext = currentLine < csvFileSection.linesToRead && csvIterator.hasNext();
            //if (!hasNext)
            //    close();
            //return hasNext;
            if (iterator == null)
                iterator = records.iterator();

            return iterator.hasNext();
        }

        @Override
        public CSVRecord next() {
            //final CSVRecord record = csvIterator.next();
            //++currentLine;
            //return record;
            return iterator.next();
        }

        private void close() {
            try {
                //final boolean isDeleted = new File(fileName + instanceID).delete();
                //if (isDeleted) {
                //    LOG.warn("Could not delete file: " + (fileName + instanceID));
                //}
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
    public void computeLocalFileSection(final int numNodes, final int instanceID) {
        final long totalLines = getNumberOfLines();
        final long numLinesPerSection = totalLines / numNodes;
        final long linesToRead = (instanceID == (numNodes - 1)) ?
                numLinesPerSection + (totalLines % numLinesPerSection) : numLinesPerSection;
        try {
            final long blockLineOffset = numLinesPerSection * instanceID;
            final BufferedReader br = new BufferedReader(new FileReader(fileName));

            br.mark(0);

            long startOffset = 0;
            for (int i = 0; i < totalLines; ++i) {
                if (i < blockLineOffset)
                    startOffset += br.readLine().length() + 1;
                else break;
            }

            // -----------------------------------------------------
                /*br.reset();
                br.skip(startOffset);
                BufferedWriter bw = new BufferedWriter(new FileWriter(fileName + instanceID));
                for (int i = 0; i < linesToRead; ++i) {
                    bw.write(br.readLine() + "\n");
                }
                bw.close();*/
            // -----------------------------------------------------

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
