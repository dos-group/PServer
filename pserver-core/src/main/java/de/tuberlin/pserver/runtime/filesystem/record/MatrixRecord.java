package de.tuberlin.pserver.runtime.filesystem.record;

import de.tuberlin.pserver.runtime.state.mtxentries.MatrixEntry;
import de.tuberlin.pserver.runtime.state.mtxentries.ReusableMatrixEntry;

/**
 * Created by Morgan K. Geldenhuys on 15.12.15.
 */
public abstract class MatrixRecord implements Record<MatrixEntry, ReusableMatrixEntry> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected String[] record;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    protected MatrixRecord(String[] record) {
        this.record = record;
    }

    // ---------------------------------------------------
    // Abstract Methods.
    // ---------------------------------------------------

    public abstract MatrixRecord set(String[] record, int[] projection, long row);

    public abstract MatrixRecord set(String[] record);

}
