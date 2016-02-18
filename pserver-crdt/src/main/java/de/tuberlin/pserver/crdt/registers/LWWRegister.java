package de.tuberlin.pserver.crdt.registers;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.crdt.operations.Operation;
import de.tuberlin.pserver.crdt.operations.TaggedOperation;
import de.tuberlin.pserver.runtime.driver.ProgramContext;

// In the Shapiro paper, concurrent updates are solved by concatenating MAC address to the timestamp...
// Here I am using a concurrent resolver instead
// TODO: maybe don't even bother with the Date class, just do everything in long
// Last Writer Wins Register
public class LWWRegister<T extends Comparable> extends AbstractRegister<T> implements Register<T> {
    private final ConcurrentResolver<T> resolver;
    private long timestamp;

    public LWWRegister(String id, int noOfReplicas, ProgramContext programContext, ConcurrentResolver<T> resolver) {
        super(id, noOfReplicas, programContext);
        this.resolver = resolver;
        this.timestamp = System.nanoTime();
        ready();
    }

    @Override
    public synchronized boolean set(T element) {
        Preconditions.checkState(!isFinished, "After finish() has been called on a CRDT no more changes can be made to it");

        long time = System.nanoTime();

        if(setRegister(element, time)) {
            broadcast(new TaggedOperation<>(Operation.OpType.ASSIGN, element, time));
            return true;
        }

        return false;
    }

    @Override
    protected boolean update(int srcNodeId, Operation op) {
        @SuppressWarnings("unchecked")
        TaggedOperation<T,Long> taggedOp = (TaggedOperation<T, Long>) op;

        switch(taggedOp.getType()) {
            case ASSIGN:
                return setRegister(taggedOp.getValue(), taggedOp.getTag());
            default:
                throw new IllegalArgumentException("LWWRegister CRDTs do not allow the " + op.getType() + " operation.");
        }
    }

    public synchronized void setTimestamp(long time) {
        this.timestamp = time;
    }

    public synchronized long getTimestamp() {
        return this.timestamp;
    }

    private synchronized void atomicUpdate(T element, long time) {
        super.set(element);
        setTimestamp(time);
    }

    private synchronized boolean resolveConcurrentUpdate(T element) {
        if(resolver.resolveConcurrent(element, this.get())) {
            atomicUpdate(element, getTimestamp());
            return true;
        }

        return false;
    }

    private synchronized boolean setRegister(T element, long time) {
        if(time > getTimestamp() || (time == getTimestamp() && resolveConcurrentUpdate(element))) {
            atomicUpdate(element, time);
            //broadcast(new TaggedOperation<>(Operation.OpType.ASSIGN, element, time));

            return true;
        }

        return false;
    }

}