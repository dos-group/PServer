package de.tuberlin.pserver.app.filesystem.local;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.app.filesystem.FileDataIterator;
import de.tuberlin.pserver.app.filesystem.record.CriteoRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;

public class LocalCriteoInputFile implements LocalInputFile<CriteoRecord> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(LocalCriteoInputFile.class);

    private final String filePath;

    private final LocalFileSection fileSelection = new LocalFileSection();

    private final int[] fields;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public LocalCriteoInputFile(final String filePath, int[] fields) {
        this.filePath = Preconditions.checkNotNull(filePath);
        this.fields = fields;
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
            fileSelection.set(totalLines, linesToRead, startOffset);
        } catch (IOException ioe) {
            throw new IllegalStateException(ioe);
        }
    }

    @Override
    public FileDataIterator<CriteoRecord> iterator() { return new CriteoFileDataIterator(fileSelection, fields); }

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

    private class LocalFileSection {

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

    private class CriteoFileDataIterator implements FileDataIterator<CriteoRecord> {

        private final BufferedReader reader;

        private final LocalFileSection fileSection;

        private long currentLine = 0;

        private final int[] fields;

        // ---------------------------------------------------

        public CriteoFileDataIterator(final LocalFileSection fileSection, int[] fields) {
            try {

                this.fileSection = Preconditions.checkNotNull(fileSection);
                this.reader      = new BufferedReader(new FileReader(Preconditions.checkNotNull(filePath)));
                this.fields      = Preconditions.checkNotNull(fields);

            } catch(Exception e) {
                close();
                throw new IllegalStateException(e);
            }
        }

        // ---------------------------------------------------reader

        @Override
        public void initialize() {
            try {
                Preconditions.checkState(fileSection != null);
                reader.skip(fileSection.startOffset);
            } catch(Exception e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public void reset() { currentLine = 0; }

        @Override
        public String getFilePath() { return filePath; }

        @Override
        public boolean hasNext() {
            final boolean hasNext = currentLine < fileSection.linesToRead;
            if (!hasNext)
                close();
            return hasNext;
        }

        @Override
        public CriteoRecord next() {
            final String line;
            try {
                line = reader.readLine();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            ++currentLine;
            return CriteoRecord.parse(line, "[ \t]+", fields);
        }

        // ---------------------------------------------------

        private void close() {
            try {
                if (reader != null)
                    reader.close();
            } catch (IOException ioe) {
                throw new IllegalStateException(ioe);
            }
        }
    }
}
