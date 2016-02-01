package de.tuberlin.pserver.regression;

import de.tuberlin.pserver.crdt.operations.Operation;
import de.tuberlin.pserver.matrix.crdt.MatrixAvgOperation;
import de.tuberlin.pserver.runtime.driver.ProgramContext;

public abstract class AbstractAvgReplicatedMatrix<T> extends AbstractReplicatedMatrix<T> {

    /**
     * Sole constructor
     *
     * @param crdtId         the ID of the CRDT that this replica belongs to
     * @param noOfReplicas
     * @param programContext the {@code ProgramContext} belonging to this {@code MLProgram}
     */
    protected AbstractAvgReplicatedMatrix(String crdtId, int noOfReplicas, ProgramContext programContext) {
        super(crdtId, noOfReplicas, programContext);
    }

    @Override
    protected boolean update(int srcNodeId, Operation op) {
        MatrixAvgOperation<Float> mop = (MatrixAvgOperation<Float>) op;

        //System.out.println(nodeID + " Received: " + op.getValue());
        switch (mop.getOpType()) {
            case AVERAGE:
                if(!isConverged) {
                    return makeAverage(mop.getRow(), mop.getCol(), mop.getValue());
                }
            default:
                return false;
        }
    }

    protected abstract boolean makeAverage(long row, long col, Float value);
}
