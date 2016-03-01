package de.tuberlin.pserver.runtime.filesystem.local;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.runtime.filesystem.AbstractFileIterator;
import de.tuberlin.pserver.runtime.filesystem.records.Record;
import de.tuberlin.pserver.runtime.filesystem.records.RecordIterator;

import java.io.*;

public class LocalFileIterator implements AbstractFileIterator {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final InputStream inputStream;

    private RecordIterator recordIterator;

    public final LocalFilePartition partition;

    private long currRow = 0;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public LocalFileIterator(LocalFile file) {
        this.partition = (LocalFilePartition) Preconditions.checkNotNull(file).getFilePartition();
        try {
            inputStream  = new FileInputStream(Preconditions.checkNotNull(partition.file));
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
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            for (int i = 0; i <= partition.offset; ++i)
                reader.readLine();
            recordIterator = RecordIterator.create(partition.fileFormat, inputStream);
        } catch(Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public boolean hasNext() {
        boolean hasNext = recordIterator.hasNext() && currRow < partition.linesToRead;
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
            if (inputStream != null)
                inputStream.close();
        } catch (IOException ioe) {
            throw new IllegalStateException(ioe);
        }
    }
}
