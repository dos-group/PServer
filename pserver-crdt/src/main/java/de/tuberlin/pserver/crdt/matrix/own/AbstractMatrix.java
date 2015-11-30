package de.tuberlin.pserver.crdt.matrix.own;

import de.tuberlin.pserver.crdt.AbstractCRDT;
import de.tuberlin.pserver.crdt.operations.Operation;
import de.tuberlin.pserver.runtime.RuntimeManager;
import de.tuberlin.pserver.runtime.driver.ProgramContext;

import java.io.Serializable;

public abstract class AbstractMatrix<T> extends AbstractCRDT<T> {
    protected final long rows;
    protected final long cols;

    /**
     * Sole constructor
     *
     * @param id             the ID of the CRDT that this replica belongs to
     * @param noOfReplicas
     * @param programContext the {@code RuntimeManager} belonging to this {@code MLProgram}
     */
    protected AbstractMatrix(long rows, long cols, String id, int noOfReplicas, ProgramContext programContext) {
        super(id, noOfReplicas, programContext);
        this.rows = rows;
        this.cols = cols;
    }


    protected static class Coordinates implements Serializable {
        private final long row;
        private final long col;

        public Coordinates(long row, long col) {
            this.row = row;
            this.col = col;
        }

        public long getRow() {
            return row;
        }

        public long getCol() {
            return col;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Coordinates that = (Coordinates) o;

            return row == that.row && col == that.col;
        }

        @Override
        public int hashCode() {
            int result = (int) (row ^ (row >>> 32));
            result = 31 * result + (int) (col ^ (col >>> 32));
            return result;
        }
    }

    protected void checkBounds(long row, long col) {
        if(row > rows || row < 0 || col > cols || col < 0) {
            throw new IndexOutOfBoundsException("The coordinates (" + row + "," + col +") are out of bounds for a "
                    + rows + "x" + cols + " matrix");
        }
    }

}
