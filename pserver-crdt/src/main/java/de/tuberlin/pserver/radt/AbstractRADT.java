package de.tuberlin.pserver.radt;

import de.tuberlin.pserver.AbstractReplicatedDataType;
import de.tuberlin.pserver.operations.Operation;
import de.tuberlin.pserver.runtime.driver.ProgramContext;
import de.tuberlin.pserver.runtime.events.MsgEventHandler;

import java.util.Arrays;
import java.util.PriorityQueue;
import java.util.Queue;

public abstract class AbstractRADT<T> extends AbstractReplicatedDataType<T> implements RADT {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected final int[] vectorClock;

    // priority queue
    private final Queue<RADTOperation<CObject<T>>> queue;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    protected AbstractRADT(String id, int noOfReplicas, ProgramContext programContext) {

        super(id, noOfReplicas, programContext);

        // Initialize vector clock
        this.vectorClock = new int[noOfReplicas];

        Arrays.fill(vectorClock, 0);

        // Initialize queue
        this.queue = new PriorityQueue<>((o1, o2) -> {

            if (o1.getS4Vector().precedes(o2.getS4Vector())) return -1;

            else if (o2.getS4Vector().precedes(o1.getS4Vector())) return 1;

            else return 0;

        });

        runtimeManager.addMsgEventListener("Operation_" + id, new MsgEventHandler() {

            @Override

            public synchronized void handleMsg(int srcNodeID, Object value) {

                if (value instanceof Operation) {

                    //Suppress the unchecked warning cause by generics cast from object to Operation<T>
                    @SuppressWarnings("unchecked")
                    Operation op = (Operation) value;

                    if(op.getType() == Operation.OpType.END) {

                        addFinishedNode(srcNodeID);

                        synchronized (AbstractRADT.this) {

                            AbstractRADT.this.notifyAll();

                        }

                    } else {

                        @SuppressWarnings("unchecked")
                        RADTOperation<CObject<T>> radtOp = (RADTOperation<CObject<T>>) value;

                        queue.add(radtOp);

                        while (queue.peek() != null && isCausallyReadyFor(queue.peek())) {

                            radtOp = queue.poll();

                            updateVectorClock(radtOp.getVectorClock());

                            update(srcNodeID, radtOp);

                        }

                    }

                }

            }

        });

    }

    public Queue<RADTOperation<CObject<T>>> getQueue() {

        return this.queue;

    }


    // ---------------------------------------------------
    // Protected Methods.
    // ---------------------------------------------------

    protected synchronized boolean isCausallyReadyFor(RADTOperation<CObject<T>> op) {

        boolean condition1 = (vectorClock[op.getS4Vector().getSiteId()] + 1) == op.getVectorClock()[op.getS4Vector().getSiteId()];

        boolean condition2 = condTwo(op);

        return condition1 && condition2;

    }

    protected synchronized int[] increaseVectorClock() {

        vectorClock[nodeId]++;

        return vectorClock.clone();

    }

    protected synchronized void updateVectorClock(int[] remoteVectorClock) {

        for(int i = 0; i < remoteVectorClock.length; i++) {

            vectorClock[i] = Math.max(vectorClock[i], remoteVectorClock[i]);

        }

    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private synchronized boolean condTwo(RADTOperation<CObject<T>> op) {

        for (int i = 0; i < vectorClock.length; i++) {

            if(i != op.getS4Vector().getSiteId()) {

                if(op.getVectorClock()[i] > vectorClock[i]) return false;

            }

        }

        return true;
    }

}
