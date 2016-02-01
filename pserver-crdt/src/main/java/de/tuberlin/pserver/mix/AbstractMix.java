package de.tuberlin.pserver.mix;

import de.tuberlin.pserver.AbstractReplicatedDataType;
import de.tuberlin.pserver.crdt.operations.Operation;
import de.tuberlin.pserver.radt.CObject;
import de.tuberlin.pserver.radt.RADT;
import de.tuberlin.pserver.radt.RADTOperation;
import de.tuberlin.pserver.runtime.driver.ProgramContext;
import de.tuberlin.pserver.runtime.events.MsgEventHandler;

import java.util.Arrays;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * Name ideas:
 *  - complex crdt
 *  - stateful crdt
 *  - iterative crdt
 *  - ...
 */

/**
 * Implementation ideas:
 *  - average
 *  - sum
 *  - max
 *  - min
 *  -
 */

public abstract class AbstractMix<T> extends AbstractReplicatedDataType<T> implements RADT {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    // priority queue
    private final Queue<MixOperation<T>> queue;

    protected int sessionID;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    protected AbstractMix(String id, int noOfReplicas, ProgramContext programContext) {
        super(id, noOfReplicas, programContext);

        // Initialize queue
        // TODO: initial capacity
        this.queue = new PriorityBlockingQueue<>(10, (o1, o2) -> o1.getSessionID() - o2.getSessionID());

        // Initialize session ID
        this.sessionID = 1;

        // TODO: this is not thread-safe
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
                        @SuppressWarnings("unchecked")
                        MixOperation<T> mixOp = (MixOperation<T>) value;
                        queue.add(mixOp);

                        while(queue.peek() != null) {
                            if(isCausallyReadyFor(queue.peek())) {
                                mixOp = queue.poll();
                                update(srcNodeID, mixOp);
                            }
                            else if(isCausallyPast(queue.peek())) {
                                // Dispose of the operation, it is outdated
                                queue.poll();
                            }
                            else if(isCausallyBehind(queue.peek())) {
                                // Catch up to the next
                                mixOp = queue.poll();
                                sessionID = mixOp.getSessionID();
                                newSession();
                                update(srcNodeID, mixOp);
                            }
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

    protected boolean isCausallyReadyFor(MixOperation<T> op) {
        System.out.println();
        System.out.println("Local session nunber: " + sessionID);
        System.out.println("Remote session number: " + op.getSessionID());
        return this.sessionID == op.getSessionID();
    }

    protected boolean isCausallyPast(MixOperation<T> op) {
        return this.sessionID > op.getSessionID();
    }

    protected boolean isCausallyBehind(MixOperation<T> op) {
        return this.sessionID < op.getSessionID();
    }

    abstract void newSession();

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------
}
