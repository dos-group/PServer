package de.tuberlin.pserver.runtime.filesystem.local;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.runtime.filesystem.FileDataIterator;
import de.tuberlin.pserver.runtime.filesystem.record.IRecord;
import de.tuberlin.pserver.runtime.filesystem.record.RecordFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.Iterator;

public class LocalInputFile implements ILocalInputFile<IRecord> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(LocalInputFile.class);

    private final String filePath;

    private final RecordFormat format;

    private final LocalCSVFileSection csvFileSection;

    public LocalInputFile(final String filePath) {
        this(filePath, RecordFormat.DEFAULT);
    }

    public LocalInputFile(final String filePath, RecordFormat format) {
        Preconditions.checkNotNull(filePath);
        Preconditions.checkNotNull(format);
        this.filePath = filePath;
        this.format   = format;
        this.csvFileSection = new LocalCSVFileSection();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void computeLocalFileSection(final int numNodes, final int nodeID) {
        final long totalLines = getNumberOfLines();
        final long numLinesPerSection = totalLines / numNodes;
        final long linesToRead = (nodeID == (numNodes - 1)) ?
                numLinesPerSection + (totalLines % numLinesPerSection) : numLinesPerSection;
        try {
            final long blockLineOffset = numLinesPerSection * nodeID;
            BufferedReader br = new BufferedReader(new FileReader(filePath));
            int eolcc = getLineEndingCharCount(br);
            br.close();
            br = new BufferedReader(new FileReader(filePath));
            long startOffset = 0;
            for (int i = 0; i < totalLines; ++i) {
                if (i < blockLineOffset)
                    startOffset += br.readLine().length() + eolcc;
                else
                    break;
            }
            br.close();
            csvFileSection.set(totalLines, linesToRead, startOffset, blockLineOffset);
        } catch (IOException ioe) {
            throw new IllegalStateException(ioe);
        }
    }

    @Override
    public FileDataIterator<IRecord> iterator() { return new CSVFileDataIterator(csvFileSection); }

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

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    private class LocalCSVFileSection {

        public long totalLines  = 0;

        public long linesToRead = 0;

        public long startOffset = 0;

        public long blockLineOffset = 0;

        public void set(long totalLines, long linesToRead, long startOffset, long blockLineOffset) {
            this.totalLines = totalLines;
            this.linesToRead = linesToRead;
            this.startOffset = startOffset;
            this.blockLineOffset = blockLineOffset;
        }
    }

    private int getLineEndingCharCount(BufferedReader br) {
        try {
            char[] buffer = new char[8192];
            int result = 0;
            while (result == 0 && br.read(buffer) > 0) {
                for (int i = 0; i < buffer.length; i++) {
                    char c = buffer[i];
                    if(c == '\n' || c == '\r') {
                        result++;
                        char c2 = 0;
                        if(i + 1 < buffer.length) {
                            c2 = buffer[i + 1];
                        }
                        else if(br.read(buffer) > 0) {
                            c2 = buffer[0];
                        }
                        if(c2 > 0 && (c2 == '\n' || c2 == '\r')) {
                            result++;
                        }
                        break;
                    }
                }
            }
            if(result <= 0 || result > 2) {
                throw new IllegalStateException("line ending char count = " + result);
            }
            return result;
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    // ---------------------------------------------------

    private class CSVFileDataIterator implements FileDataIterator<IRecord> {

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
                this.csvFileParser  = new CSVParser(fileReader, format.getCsvFormat());
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

        IRecord reusable = format.getRecordFactory().wrap(null, null, -1);

        @Override
        public IRecord next() {
            final CSVRecord record = csvIterator.next();
            return reusable.set(record, format.getProjection(), csvFileSection.blockLineOffset + currentLine++);
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
    }

}
