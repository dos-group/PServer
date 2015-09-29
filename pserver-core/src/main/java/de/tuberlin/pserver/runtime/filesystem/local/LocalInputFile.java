package de.tuberlin.pserver.runtime.filesystem.local;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.runtime.filesystem.FileDataIterator;
import de.tuberlin.pserver.runtime.filesystem.FileSection;
import de.tuberlin.pserver.runtime.filesystem.record.IRecord;
import de.tuberlin.pserver.runtime.filesystem.record.config.AbstractRecordFormatConfig;
import de.tuberlin.pserver.runtime.partitioning.IMatrixPartitioner;
import de.tuberlin.pserver.types.PartitionType;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.util.Iterator;

public class LocalInputFile implements ILocalInputFile<IRecord> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final String filePath;

    private final AbstractRecordFormatConfig format;

    private final FileSection fileSection;

    private final IMatrixPartitioner partitioner;


    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public LocalInputFile(final String filePath,
                          final AbstractRecordFormatConfig format,
                          final IMatrixPartitioner partitioner) {

        this.filePath       = Preconditions.checkNotNull(filePath);
        this.format         = Preconditions.checkNotNull(format);
        this.partitioner    = partitioner;
        this.fileSection    = new FileSection();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void computeLocalFileSection(final int numNodes, final int nodeID) {
        final long totalLines = getNumberOfLines();
        // we assume here, that number of row-partitions determine file splitting. This is not necessarily true i.e. if a line in a file contains row-values for one column.
        // RecordFormatConfig should give a hint for that
        if(partitioner.getNumRowPartitions() == 1) {
            fileSection.set(totalLines, totalLines, 0, totalLines);
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
    public FileDataIterator<IRecord> iterator() { return new CSVFileDataIterator(fileSection); }

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

        private final FileSection fileSection;

        private long currentLine = 0;

        // ---------------------------------------------------

        public CSVFileDataIterator(final FileSection fileSection) {
            try {

                this.fileSection = Preconditions.checkNotNull(fileSection);
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
                if (fileSection != null)
                    fileReader.skip(fileSection.startOffset);
                else
                    throw new IllegalStateException();
            } catch(Exception e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public void reset() { /*iterator = null;*/ }

        @Override
        public String getFilePath() { return filePath; }

        @Override
        public FileSection getFileSection() {
            return fileSection;
        }

        @Override
        public boolean hasNext() {
            final boolean hasNext = currentLine < fileSection.linesToRead && csvIterator.hasNext();
            if (!hasNext)
                close();
            return hasNext;
        }

        IRecord reusableRecord = format.getRecordFactory().wrap(null, null, -1);

        @Override
        public IRecord next() {
            final CSVRecord record = csvIterator.next();
            return reusableRecord.set(record, format.getProjection(), fileSection.blockLineOffset + currentLine++);
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
