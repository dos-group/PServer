package de.tuberlin.pserver.runtime.filesystem.records;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;


public class SVMRecordIterator extends RecordIterator<Record> {

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
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        //this.row++;
        if (this.reusableRecord == null)
            return this.reusableRecord = new SVMRecord(this.row++, line, this.projection);
        else
            return this.reusableRecord.set(this.row++, line, this.projection);
    }

    @Override
    public Record next(long lineNum) {
        String line;
        try {
            line = reader.readLine();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        this.row = lineNum;
        if (this.reusableRecord == null)
            return this.reusableRecord = new SVMRecord(this.row, line, this.projection);
        else
            return this.reusableRecord.set(this.row, line, this.projection);
    }
}
