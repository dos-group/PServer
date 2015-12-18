package de.tuberlin.pserver.runtime.filesystem.record;

import de.tuberlin.pserver.runtime.state.mtxentries.MatrixEntry;
import de.tuberlin.pserver.runtime.state.mtxentries.ReusableMatrixEntry;
import org.apache.commons.csv.CSVRecord;

/**
 * Created by Morgan K. Geldenhuys on 15.12.15.
 */
public abstract class MatrixRecord implements Record<MatrixEntry, ReusableMatrixEntry> {

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
