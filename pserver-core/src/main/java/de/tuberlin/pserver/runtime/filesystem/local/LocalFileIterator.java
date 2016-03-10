package de.tuberlin.pserver.runtime.filesystem.local;


import com.google.common.base.Preconditions;
import com.google.common.io.CountingInputStream;
import de.tuberlin.pserver.runtime.filesystem.AbstractFileIterator;
import de.tuberlin.pserver.runtime.filesystem.records.Record;
import de.tuberlin.pserver.runtime.filesystem.records.RecordIterator;
import de.tuberlin.pserver.types.matrix.typeinfo.MatrixTypeInfo;

import java.io.*;

public class LocalFileIterator implements AbstractFileIterator {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final LocalFile file;

    private final LocalFileIterationContext ic;

    private RecordIterator recordIterator;

    private long currRow = 0;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public LocalFileIterator(LocalFile file) {
        this.file = file;
        this.ic = new LocalFileIterationContext((LocalFilePartition) Preconditions.checkNotNull(file).getFilePartition());
        try {
            this.ic.inputStream = new CountingInputStream(new FileInputStream(Preconditions.checkNotNull(file.getTypeInfo().input().filePath())));
        } catch(Exception e) {
            close();
            throw new IllegalStateException(e);
        }
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void open() {
        try {

            int lineCount = 0;
            while (lineCount < ic.partition.localBlock.offset)
                if (ic.inputStream.read() == '\n')
                    lineCount++;

            recordIterator = RecordIterator.create((MatrixTypeInfo) file.getTypeInfo(), ic);
        } catch(Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public boolean hasNext() {
        boolean hasNext = recordIterator.hasNext() && currRow < ic.partition.localBlock.linesToRead;
        if (!hasNext)
            close();
        return hasNext;
    }

    @Override
    public Record next() {
        ++currRow;
        return recordIterator.next();
    }

    @Override
    public void close() {
        try {
            if (ic.inputStream != null)
                ic.inputStream.close();
        } catch (IOException ioe) {
            throw new IllegalStateException(ioe);
        }
    }
}
