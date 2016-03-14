package de.tuberlin.pserver.crdt.registers;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.operations.Operation;
import de.tuberlin.pserver.operations.TaggedOperation;
import de.tuberlin.pserver.runtime.driver.ProgramContext;

// Last Writer Wins Register
public class LWWRegister<T extends Comparable> extends AbstractRegister<T> implements Register<T> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private long timestamp;

    private int currSrcNodeId = -1;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public LWWRegister(String id, int noOfReplicas, ProgramContext programContext) {

        super(id, noOfReplicas, programContext);

        this.timestamp = System.nanoTime();

        ready();

    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public synchronized boolean set(T element) {

        Preconditions.checkState(!isFinished, "After finish() has been called on a CRDT no more changes can be made to it");

        long time = System.nanoTime();

        if(setRegister(element, time, nodeId)) {

            broadcast(new TaggedOperation<>(Operation.OpType.ASSIGN, element, time));

            return true;

        }

        return false;

    }

    public synchronized void setTimestamp(long time) {

        this.timestamp = time;

    }

    public synchronized long getTimestamp() {

        return this.timestamp;

    }

    // ---------------------------------------------------
    // Protected Methods.
    // ---------------------------------------------------

    @Override
    protected boolean update(int srcNodeId, Operation op) {

        @SuppressWarnings("unchecked")
        TaggedOperation<T,Long> taggedOp = (TaggedOperation<T, Long>) op;

        switch(taggedOp.getType()) {

            case ASSIGN:

                return setRegister(taggedOp.getValue(), taggedOp.getTag(), srcNodeId);

            default:

                throw new IllegalArgumentException("LWWRegister CRDTs do not allow the " + op.getType() + " operation.");

        }

    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private synchronized void atomicUpdate(T element, long time, int srcNodeId) {

        super.set(element);

        setTimestamp(time);

        this.currSrcNodeId = srcNodeId;

    }

    private synchronized boolean resolveConcurrentUpdate(T element, int srcNodeId) {

        //if(resolver.resolveConcurrent(element, this.get())) {

        return currSrcNodeId < srcNodeId;

    }

    private synchronized boolean setRegister(T element, long time, int srcNodeId) {

        if(time > getTimestamp() || (time == getTimestamp() && resolveConcurrentUpdate(element, srcNodeId))) {

            atomicUpdate(element, time, srcNodeId);

            return true;

        }

        return false;

    }

}