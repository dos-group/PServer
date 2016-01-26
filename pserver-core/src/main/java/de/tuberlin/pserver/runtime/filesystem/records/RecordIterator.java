package de.tuberlin.pserver.runtime.filesystem.records;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.runtime.filesystem.FileFormat;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.Optional;


public abstract class RecordIterator<T extends Record> implements Iterator<T> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected BufferedReader reader;
    protected Optional<long[]> projection;
    protected long row;
    protected Record reusableRecord;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    protected RecordIterator(InputStream inputStream, Optional<long[]> projection) {
        this.reader = new BufferedReader(new InputStreamReader(Preconditions.checkNotNull(inputStream)));
        this.projection = projection;
        this.row = 0;
    }

    // ---------------------------------------------------
    // Factory Methods.
    // ---------------------------------------------------

    public static RecordIterator create(FileFormat fileFormat, InputStream inputStream) {
        return RecordIterator.create(fileFormat, inputStream, Optional.<long[]>empty());
    }

    public static RecordIterator create(FileFormat fileFormat, InputStream inputStream, Optional<long[]> projection) {
        switch(fileFormat) {
            case SVM_FORMAT:    SVMRecord.SVMParser.setFileFormat(fileFormat);
                                return new SVMRecordIterator(inputStream, projection);
            default:            throw new IllegalArgumentException("Unknown File Format");
        }
    }

    // ---------------------------------------------------
    // Abstract Methods.
    // ---------------------------------------------------

    public abstract T next();

    public abstract T next(long lineNum);
}
