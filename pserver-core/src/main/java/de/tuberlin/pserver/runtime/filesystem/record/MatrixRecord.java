package de.tuberlin.pserver.runtime.filesystem.record;

import de.tuberlin.pserver.runtime.state.entries.MatrixEntry;
import de.tuberlin.pserver.runtime.state.entries.ReusableMatrixEntry;
import org.apache.commons.csv.CSVRecord;

/**
 * Created by Morgan K. Geldenhuys on 15.12.15.
 */
public abstract class MatrixRecord<E extends MatrixEntry, R extends ReusableMatrixEntry> implements Record<E, R> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected CSVRecord delegate;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    protected MatrixRecord(CSVRecord delegate) {
        this.delegate = delegate;
    }

    // ---------------------------------------------------
    // Abstract Methods.
    // ---------------------------------------------------

    public abstract MatrixRecord set(CSVRecord csvRecord, int[] projection, long row);

    public abstract MatrixRecord set(CSVRecord csvRecord);

}
