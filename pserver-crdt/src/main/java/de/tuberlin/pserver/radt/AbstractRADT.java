package de.tuberlin.pserver.radt;

import de.tuberlin.pserver.AbstractReplicatedDataType;
import de.tuberlin.pserver.crdt.AbstractCRDT;
import de.tuberlin.pserver.crdt.operations.Operation;
import de.tuberlin.pserver.runtime.RuntimeManager;
import de.tuberlin.pserver.runtime.events.MsgEventHandler;

import java.util.Arrays;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Queue;

public abstract class AbstractRADT<T> extends AbstractReplicatedDataType<T> implements RADT<T> {
    protected final int[] vectorClock;
    protected final int siteID;
    // priority queue
    protected final Queue<RADTOperation<CObject<T>>> queue;
    // TODO: Not sure what this does...
    protected int sessionID;
    protected final int size;


    protected AbstractRADT(int size, String id, int noOfReplicas, RuntimeManager runtimeManager) {
        super(id, noOfReplicas, runtimeManager);

        // Remove the

        // Initialize size
        // TODO: do we need size?
        this.size = size;

        // Initialize vector clock
        this.vectorClock = new int[runtimeManager.getNodeIDs().length];
        Arrays.fill(vectorClock, 0);

        // Initialize site ID
        // TODO: we need a getNodeID() function in runtimeManager
        this.siteID = getNodeID(runtimeManager);

        // Initialize queue
        this.queue = new PriorityQueue<>((Operation<CObject<T>> o1, Operation<CObject<T>> o2) -> {
            if (o1.getValue().getS4Vector().takesPrecedenceOver(o2.getValue().getS4Vector())) {
                return -1;
            } else if (o2.getValue().getS4Vector().takesPrecedenceOver(o1.getValue().getS4Vector())) {
                return 1;
            } else {
                return 0;
            }
        });

        // Initialize session ID
        // TODO: what is this for?
        this.sessionID = 0;

        runtimeManager.addMsgEventListener("Operation_" + id, new MsgEventHandler() {
            @Override
            public void handleMsg(int srcNodeID, Object value) {
                if (value instanceof Operation) {
                    @SuppressWarnings("unchecked")
                    Operation op = (Operation) value;

                    if(op.getType() == Operation.END) {
                        addFinishedNode(srcNodeID);
                    } else {
                        RADTOperation<CObject<T>> radtOp = (RADTOperation < CObject < T >>) value;
                        queue.add(radtOp);

                        while(queue.peek() != null && isCausallyReadyFor(queue.peek())) {
                            radtOp = queue.poll();
                            updateVectorClock(radtOp.getVectorClock());
                            update(srcNodeID, radtOp);
                        }
                    }
                    //Suppress the unchecked warning cause by generics cast from object to Operation<T>
                }
            }
        });


        // Start the RADT
        ready();
    }

    // TODO: this method is stupid, there should be a public getNodeID in runtimeManager
    private int getNodeID(RuntimeManager runtimeManager) {
        int[] a = runtimeManager.getNodeIDs();
        int[] b = runtimeManager.getRemoteNodeIDs();

        for (int anA : a) {
            boolean found = false;
            for (int aB : b) {
                if (anA == aB) {
                    found = true;
                }
                if (!found) {
                    return anA;
                }
            }
        }
        return -1;
    }

    protected boolean isCausallyReadyFor(RADTOperation<CObject<T>> op) {
        // TODO: what about if sum > vectorClockSum => must the operation be purged from queue?
        // TODO: this needs verification
        System.out.println();
        System.out.println("Local vector clock: " + vectorClock[op.getS4Vector().getSiteId()]);
        System.out.println("Remote vector clock: " + op.getVectorClock()[op.getS4Vector().getSiteId()]);
        return vectorClock[op.getS4Vector().getSiteId()] == (op.getVectorClock()[op.getS4Vector().getSiteId()] - 1);
    }

    protected int[] increaseVectorClock() {
        vectorClock[siteID]++;
        return vectorClock.clone();
    }

    protected void updateVectorClock(int[] remoteVectorClock) {
        for(int i = 0; i < remoteVectorClock.length; i++) {
            vectorClock[i] = Math.max(vectorClock[i], remoteVectorClock[i]);
        }
    }
}
