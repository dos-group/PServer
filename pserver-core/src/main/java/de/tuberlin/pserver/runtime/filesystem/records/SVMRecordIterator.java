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
        return this.row < this.lines.size64();
    }

    @Override
    public Record next() {
        String line = this.lines.get(this.row);
        this.row++;
        if (this.reusableRecord == null)
            return this.reusableRecord = new SVMRecord(this.row, line, this.projection);
        return this.reusableRecord.set(this.row, line, this.projection);
    }

    @Override
    public Record next(long lineNumber) {
        String line = this.lines.get(lineNumber);
        this.row = lineNumber + 1;
        if (this.reusableRecord == null)
            return this.reusableRecord = new SVMRecord(this.row, line, this.projection);
        return this.reusableRecord.set(this.row, line, this.projection);
    }

}
