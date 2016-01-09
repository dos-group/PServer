package de.tuberlin.pserver.runtime.filesystem.records;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.runtime.filesystem.FileFormat;
import it.unimi.dsi.fastutil.BigList;
import it.unimi.dsi.fastutil.objects.ObjectBigArrayBigList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

import java.util.Optional;
import java.util.stream.Stream;

/**
 * Created by Morgan K. Geldenhuys on 16.12.15.
 */
public abstract class RecordIterator<T extends Record> implements Iterator<T> {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    Logger LOG = LoggerFactory.getLogger(RecordIterator.class);

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected BigList<String> lines;
    protected Optional<long[]> projection;
    protected long row;
    protected Record reusableRecord;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    protected RecordIterator(InputStream inputStream, Optional<long[]> projection) {
        this.lines = new ObjectBigArrayBigList<>();
        Stream<String> linesStream =
            new BufferedReader(new InputStreamReader(
                Preconditions.checkNotNull(inputStream),
                StandardCharsets.UTF_8)).lines();
        linesStream.forEach(line -> this.lines.add(line));
        this.projection = projection;
        this.row = 0;
    }

    // ---------------------------------------------------
    // Factory Methods.
    // ---------------------------------------------------

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

    public abstract T next(long lineNumber);

}
