package de.tuberlin.pserver.runtime.filesystem.local;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.runtime.filesystem.FileDataIterator;
import de.tuberlin.pserver.runtime.filesystem.FileFormat;
import de.tuberlin.pserver.runtime.filesystem.FileSection;
import de.tuberlin.pserver.runtime.filesystem.records.Record;
import de.tuberlin.pserver.runtime.filesystem.records.RecordIterator;
import de.tuberlin.pserver.types.matrix.implementation.partitioner.AbstractMatrixPartitioner;

import java.io.*;

public class LocalInputFile implements ILocalInputFile<Record> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final String filePath;

    private final FileFormat fileFormat;

    private final FileSection fileSection;

    private final AbstractMatrixPartitioner partitioner;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public LocalInputFile(final String filePath,
                          final FileFormat fileFormat,
                          final AbstractMatrixPartitioner partitioner) {

        this.filePath       = Preconditions.checkNotNull(filePath);
        this.fileFormat     = Preconditions.checkNotNull(fileFormat);
        this.partitioner    = partitioner;
        this.fileSection    = new FileSection();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void computeLocalFileSection(final int numNodes, final int nodeID) {
        final long totalLines = getNumberOfLines();
        // We assume here, that number of row-partitions determine file splitting. This is not necessarily
        // true i.e. if a line in a file contains row-values for one column. RecordFormatConfig should give
        // a hint for that.
        if(partitioner.getNumRowPartitions() == 1) {
            fileSection.set(totalLines, totalLines, 0, 0); // TODO: Here is the error....
        }
        else {
            try {
                final long numLinesPerSection = totalLines / numNodes;
                final long linesToRead = (nodeID == (numNodes - 1)) ?
                        numLinesPerSection + (totalLines % numLinesPerSection) : numLinesPerSection;
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
                fileSection.set(totalLines, linesToRead, startOffset, blockLineOffset);
            } catch (IOException ioe) {
                throw new IllegalStateException(ioe);
            }
        }
    }

    @Override
    public FileDataIterator<Record> iterator() {
        return new LocalFileDataIterator(fileSection);
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private long getNumberOfLines() {
        try {
            final LineNumberReader lnr = new LineNumberReader(new FileReader(filePath));
            lnr.skip(Long.MAX_VALUE);
            final int numLines = lnr.getLineNumber();
            lnr.close();
            return  numLines + 1;  // TODO: The first or the last line seems to be not counted...
        } catch(IOException ioe) {
            throw new IllegalStateException(ioe);
        }
    }

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

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

    private class LocalFileDataIterator implements FileDataIterator<Record> {

        private final InputStream inputStream;

        private RecordIterator recordIterator;

        private long currentLine = 0;

        // ---------------------------------------------------

        public LocalFileDataIterator(final FileSection fileSection) {
            try {
                inputStream  = new FileInputStream(Preconditions.checkNotNull(filePath));
            } catch(Exception e) {
                close();
                throw new IllegalStateException(e);
            }
        }

        // ---------------------------------------------------

        @Override
        public void initialize() {
            try {
                inputStream.skip(fileSection.startOffset);
                recordIterator = RecordIterator.create(fileFormat, inputStream);
            }
            catch(Exception e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public void reset() {}

        @Override
        public String getFilePath() { return filePath; }

        @Override
        public FileSection getFileSection() {
            return fileSection;
        }

        @Override
        public boolean hasNext() {
            final boolean hasNext = currentLine < fileSection.linesToRead && recordIterator.hasNext();
            if (!hasNext)
                close();
            return hasNext;
        }

        @Override
        public Record next() {
            return recordIterator.next(fileSection.blockLineOffset + currentLine++);
        }

        // ---------------------------------------------------

        private void close() {
            try {
                if (inputStream != null)
                    inputStream.close();
            } catch (IOException ioe) {
                throw new IllegalStateException(ioe);
            }
        }
    }
}
