package de.tuberlin.pserver.crdt.matrix;

import de.tuberlin.pserver.crdt.operations.Operation;
import de.tuberlin.pserver.crdt.operations.TaggedOperation;
import de.tuberlin.pserver.runtime.RuntimeManager;
import de.tuberlin.pserver.runtime.driver.ProgramContext;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

// Dictionary of Keys approach
// https://en.wikipedia.org/wiki/Sparse_matrix#Special_structure

public class SparseMatrix<T> extends AbstractMatrix<T> {


    Map<Coordinates, Float> nonZero;

    /**
     * Sole constructor
     *
     * @param id             the ID of the CRDT that this replica belongs to
     * @param noOfReplicas
     * @param programContext the {@code RuntimeManager} belonging to this {@code MLProgram}
     */
    protected SparseMatrix(Long rows, Long cols, String id, int noOfReplicas, ProgramContext programContext) {
        super(rows, cols, id, noOfReplicas, programContext);
        this.nonZero = new HashMap<>();
    }

    @Override
    protected boolean update(int srcNodeId, Operation<?> op) {
        TaggedOperation<Float, Coordinates> mop = (TaggedOperation<Float, Coordinates>) op;

        if (mop.getType() == Operation.SET) {
            return setValue(mop.getTag().getRow(), mop.getTag().getCol(), mop.getValue());
        } else {
            throw new IllegalArgumentException("GCounter CRDTs do not allow the " + op.getOperationType() + " operation.");
        }
    }

    // TODO: bounds check if row or col is larger than matrix size
    public Float get(long row, long col) {
        checkBounds(row, col);
        return nonZero.get(new Coordinates(row, col));
    }

    public boolean set(long row, long col, float value) {
        if(setValue(row, col, value)) {
            broadcast(new TaggedOperation<>(Operation.SET, value, new Coordinates(row, col) ));
            return true;
        }
        return false;
    }

    private boolean setValue(long row, long col, float value) {
        checkBounds(row, col);

        if(value == 0) {
            nonZero.remove(new Coordinates(row, col));
        }
        else {
            nonZero.put(new Coordinates(row, col), value);
        }

        return true;
    }
}
