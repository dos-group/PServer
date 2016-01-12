package de.tuberlin.pserver.runtime.filesystem.records;

import java.io.*;
import java.util.Optional;

/**
 * Created by Morgan K. Geldenhuys on 17.12.15.
 */
public class SVMRecordIterator<V extends Number> extends RecordIterator<Record> {

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public SVMRecordIterator(InputStream inputStream, Optional<long[]> projection) {
        super(inputStream, projection);
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
            }
            else {
                reader.reset();
                return true;
            }
        }
        catch (IOException e) {
            return false;
        }
    }

    @Override
    public Record next() {
        String line;
        try {
            line = reader.readLine();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        this.row++;
        if (this.reusableRecord == null)
            return this.reusableRecord = new SVMRecord(this.row, line, this.projection);
        return this.reusableRecord.set(this.row, line, this.projection);
    }

}
