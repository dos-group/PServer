package de.tuberlin.pserver.mix;

import de.tuberlin.pserver.crdt.operations.Operation;
import de.tuberlin.pserver.matrix.MatrixOperation;
import de.tuberlin.pserver.runtime.driver.ProgramContext;

public class AverageMix<T> extends AbstractMix<T> {
    private double avg;
    private long count;

    protected AverageMix(String id, int noOfReplicas, ProgramContext programContext) {
        super(id, noOfReplicas, programContext);
        this.avg = 0;
        this.count = 0;
    }

    @Override
    protected boolean update(int srcNodeId, Operation op) {
        @SuppressWarnings("unchecked")
        MixOperation<Integer> mixOp = (MixOperation<Integer>) op;

        if(mixOp.getOpType() == MixOperation.MixOperationType.AVG) {
            avg = (avg * count + mixOp.getValue()) / ++count;
            return true;
        }
        else {
            // TODO: exception text
            throw new IllegalArgumentException("HashTable RADTs do not allow the " + op.getType() + " operation.");
        }
    }

    public void average(int value) {
        avg = (avg * count + value) / ++count;
        broadcast(new MixOperation<>(sessionID, value));
    }

    // TODO: what if count is 0
    public double getAvg() {
        return avg;
    }

    public void newSession() {
        avg = 0;
        count = 0;
    }

    public void incSessionCount() {
        sessionID++;
    }
}
