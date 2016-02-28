package de.tuberlin.pserver.runtime.filesystem.records;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

public class SVMRecordIterator extends RecordIterator {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private long row;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public SVMRecordIterator(InputStream inputStream, Optional<long[]> projection) {
        super(inputStream, projection);
        reusableRecord = new SVMRecord(this.projection);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public boolean hasNext() {
        try {
            reader.mark(1);
            if (reader.read() < 0) {
                this.reader.close();
                return false;
            } else {
                reader.reset();
                return true;
            }
        } catch (IOException e) {
            return false;
        }
    }

    @Override
    public Record next() {
        try {
            return this.reusableRecord.set(row++, reader.readLine());
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
