package de.tuberlin.pserver.runtime.filesystem.record;

import de.tuberlin.pserver.runtime.RuntimeManager;
import de.tuberlin.pserver.runtime.partitioning.mtxentries.MatrixEntry;
import de.tuberlin.pserver.runtime.partitioning.mtxentries.ReusableMatrixEntry;
import org.apache.commons.csv.CSVRecord;

import java.util.Iterator;

/**
 * Represents a record consisting of arbitrarily many fields.
 *
 * <i>Note: A record is interpreted as a row by {@link RuntimeManager#loadFilesIntoDHT()} with its fields as column
 * values </i>
 */
public interface IRecord extends Iterator<MatrixEntry> {

    /**
     * How many fields are accessible in this record.
     * <br>
     * <i>Note: This will be interpreted as number of columns in {@link RuntimeManager#loadFilesIntoDHT()}</i>
     *
     * @return the number of fields that are accessible in this record
     */
    int size();

    /**
     * Gets the value of the i'th field of this record.
     * <br>
     * <i>Note: This will be interpreted as number of columns in {@link RuntimeManager#loadFilesIntoDHT()}</i>
     *
     * @return the value of the i'th field of this record
     */
    MatrixEntry get(int i);

    public MatrixEntry next(ReusableMatrixEntry reusable);

    public IRecord set(CSVRecord delegate, int[] projection, long row);

}
