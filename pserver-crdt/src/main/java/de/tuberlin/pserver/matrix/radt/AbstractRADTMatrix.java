package de.tuberlin.pserver.matrix.radt;

import de.tuberlin.pserver.AbstractReplicatedDataType;
import de.tuberlin.pserver.crdt.operations.Operation;
import de.tuberlin.pserver.matrix.MatrixOperation;
import de.tuberlin.pserver.runtime.driver.ProgramContext;
import de.tuberlin.pserver.runtime.events.MsgEventHandler;

import java.util.Arrays;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.PriorityBlockingQueue;

public abstract class AbstractRADTMatrix<T> extends AbstractReplicatedDataType<T> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected final long[] vectorClock;
    // priority queue
    private final Queue<MatrixOperation> queue;
    // TODO: Not sure what this does...
    protected int sessionID;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    protected AbstractRADTMatrix(String id, int noOfReplicas, ProgramContext programContext) {
        super(id, noOfReplicas, programContext);

        // Initialize vector clock
        this.vectorClock = new long[runtimeManager.getNodeIDs().length];
        Arrays.fill(vectorClock, 0);

        // Initialize queue
        // TODO: choose initial size well
        this.queue = new PriorityBlockingQueue<>(25, (o1, o2) -> {
            if (o1.getS3Vector().takesPrecedenceOver(o2.getS3Vector())) return -1;
            else if (o2.getS3Vector().takesPrecedenceOver(o1.getS3Vector())) return 1;
            else return 0;
        });

        // Initialize session ID
        this.sessionID = 0;

        runtimeManager.addMsgEventListener("Operation_" + id, new MsgEventHandler() {
            @Override
            public void handleMsg(int srcNodeID, Object value) {
                if (value instanceof Operation) {
                    //Suppress the unchecked warning cause by generics cast from object to Operation<T>
                    @SuppressWarnings("unchecked")
                    Operation op = (Operation) value;

                    if(op.getType() == Operation.OpType.END) {
                        addFinishedNode(srcNodeID);
                    } else {
                        //System.out.println("[DEBUG:" + nodeId + "] Received: " + op.getValue());

                        @SuppressWarnings("unchecked")
                        MatrixOperation mop = (MatrixOperation) value;
                        queue.add(mop);

                        while(queue.peek() != null && isCausallyReadyFor(queue.peek())) {
                            mop = queue.poll();
                            updateVectorClock(mop.getVectorClock());
                            update(srcNodeID, mop);
                        }
                    }
                }
            }
        });

        // Start the RADT
        ready();
    }

    // ---------------------------------------------------
    // Protected Methods.
    // ---------------------------------------------------

    /*protected boolean isCausallyReadyFor(MatrixOperation op) {
        // TODO: what about if sum > vectorClockSum => must the operation be purged from queue?
        // TODO: this needs verification
        System.out.println();
        //System.out.println("Local vector clock: " + vectorClock[Math.toIntExact(op.getS3Vector().getSiteID())]);
        //System.out.println("Remote vector clock: " + op.getVectorClock()[Math.toIntExact(op.getS3Vector().getSiteID())]);
        return vectorClock[Math.toIntExact(op.getS3Vector().getSiteID())] == (op.getVectorClock()[Math.toIntExact(op.getS3Vector().getSiteID())]-1);
    }*/

    protected abstract boolean isCausallyReadyFor(MatrixOperation op);

    protected synchronized long[] increaseVectorClock() {
        vectorClock[nodeId]++;
        return vectorClock.clone();
    }

    protected synchronized long increaseSessionID() {
        return sessionID++;
    }

    protected synchronized void updateVectorClock(long[] remoteVectorClock) {
        for(int i = 0; i < remoteVectorClock.length; i++) {
            vectorClock[i] = Math.max(vectorClock[i], remoteVectorClock[i]);
        }
    }

    public Queue getQueue() {
        return this.queue;
    }
}
