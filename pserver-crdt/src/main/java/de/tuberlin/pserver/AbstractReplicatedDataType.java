package de.tuberlin.pserver;

import de.tuberlin.pserver.crdt.operations.EndOperation;
import de.tuberlin.pserver.crdt.operations.Operation;
import de.tuberlin.pserver.runtime.RuntimeManager;
import de.tuberlin.pserver.runtime.driver.ProgramContext;
import de.tuberlin.pserver.runtime.events.MsgEventHandler;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;


// TODO: what about exceptions in general
// TODO: what about when counters reach MAX_INT => exception or keep counting somehow?
// TODO: what if someone uses the same id for two crdts?
// TODO: what if only one node is running?
// TODO: maybe use blocking queues for buffers
// TODO: comments and documentation
// TODO: improve the P2P discovery and termination
// TODO: what if there is not one replica on every node? => pass number of replicas into constructor!?
// TODO: change constructor to CRDT.newReplica(...) to reflect that it is replicas being dealt with?
// TODO: Javadoc package descriptions

/**
 * <p>
 * This class provides a skeletal implementation of the {@code CRDT} interface, to minimize the effort required to
 * implement this interface in subclasses.
 *</p>
 *<p>
 * In particular, this class provides functionality for starting/finishing a CRDT and broadcasting/receiving updates.
 *</p>
 *<p>
 * To implement a CRDT, the programmer needs to extend this class, implement the desired local data structure of one
 * replica, call the {@code broadcast} method when operations should be sent to all replicas and provide an
 * implementation of the {@code update} method. All further communication between replicas and starting/finishing of the
 * CRDT are handled by this class.
 *</p>
 *<p>
 * When extending this class make sure to call the {@code super()} constructor which will enable the above functionalities.
 *</p>
 *
 * @param <T> the type of elements in this CRDT
 */
public abstract class AbstractReplicatedDataType<T> implements ReplicatedDataType {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(AbstractReplicatedDataType.class);

    private final Set<Integer> runningNodes;

    private final Set<Integer> finishedNodes;

    private final Queue<Operation> buffer;

    private final String crdtId;

    protected final int nodeId;

    // TODO: is programContext really necessary here?
    private final ProgramContext programContext;

    protected final RuntimeManager runtimeManager;

    private final int noOfReplicas;

    private volatile boolean allNodesRunning;

    private volatile boolean allNodesFinished;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    // TODO: do I need the whole programContext if I only use the nodeId

    /** Sole constructor
     *
     * @param crdtId the ID of the CRDT that this replica belongs to
     * @param programContext the {@code ProgramContext} belonging to this {@code MLProgram}
     * */
    protected AbstractReplicatedDataType(String crdtId, int noOfReplicas, ProgramContext programContext) {
        // Threadsafe Sets
        // TODO: performance depends on a good hash function and good size estimate (resizing is slow)
        this.runningNodes = Collections.newSetFromMap(new ConcurrentHashMap<>());
        this.finishedNodes = Collections.newSetFromMap(new ConcurrentHashMap<>());

        this.buffer = new ConcurrentLinkedQueue<>();
        this.noOfReplicas = noOfReplicas;

        this.runtimeManager = programContext.runtimeContext.runtimeManager;
        this.programContext = programContext;
        this.crdtId = crdtId;
        this.nodeId = programContext.runtimeContext.nodeID;

        this.allNodesRunning = false;
        this.allNodesFinished = false;

        runtimeManager.addMsgEventListener("Running_" + crdtId, new MsgEventHandler() {
            @Override
            public void handleMsg(int srcNodeID, Object value) {
                runningNodes.add(srcNodeID);
                LOG.info("[" + nodeId + "|crdt '" + crdtId + "'] Number of running remote replicas: " + runningNodes.size() + "//"
                        + (noOfReplicas - 1));

                if (runningNodes.size() == noOfReplicas - 1) {
                    allNodesRunning = true;
                    runtimeManager.removeMsgEventListener("Running_" + crdtId, this);

                    // This is necessary to reach replicas that were not online when the first "Running" message was sent
                    // TODO: this is so ugly! (toPrimitive())
                    runtimeManager.send("Running_" + crdtId, 0, ArrayUtils.toPrimitive(runningNodes.toArray(new Integer[0])));

                    // TODO: Start the scheduler that periodically broadcasts operations
                    // TODO: is this really a good idea? Will it not influence calculation results?
                    // ScheduledThreadPoolExecutor
                }
            }
        });

        //ready();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    /**
     * Should be called when a CRDT replica is finished producing and applying local updates. It will cause this replica
     * to wait for all replicas of this CRDT to finish and will apply any updates received to reach the replica's final
     * state. (This is a blocking call)
     */
    // This shouldn't be called from a thread in parallel execution! Only by main thread.
    // TODO: this is blocking if not all nodes start/finish...
    @Override
    public final void finish() {

        if(!allNodesRunning) LOG.info("[" + nodeId + "|crdt '" + crdtId + "']" + " Waiting for all CRDTs to start");
        while(!allNodesRunning) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        System.out.println("[DEBUG] BufferB: " + buffer.size());

        broadcast(new EndOperation());

        if(!allNodesFinished) LOG.info("[" + nodeId + "|crdt '" + crdtId + "']" + " Waiting for all CRDTs to finish");
        while(!allNodesFinished) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Gets the {@code buffer} queue associated with this CRDT replica. The buffer contains operations applied to the
     * replica locally but not yet broadcast to other replicas.
     *
     * @return a copy of the CRDTs current buffer
     */
    public final Queue getBuffer() {
        return new LinkedList<>(this.buffer);
    }

    // ---------------------------------------------------
    // Protected Methods.
    // ---------------------------------------------------

    /**
     * Broadcasts an {@code Operation} to all replicas belonging to this CRDT (same {@code crdtId}) or buffers the
     * {@code Operation} for later broadcasting if not all replicas are online yet.
     *
     * @param op the operation that should be broadcast
     */
    protected final void broadcast(Operation op) {
        // TODO: batch processing
        // send to all nodes
        if(allNodesRunning) {
            broadcastBuffer();

            // TODO: Not necessarily all nodes have replicas of this CRDT
            runtimeManager.send("Operation_" + crdtId, op, ArrayUtils.toPrimitive(runningNodes.toArray(new Integer[0])));
        } else {
            buffer(op);
        }
    }

    /**
     * Applies an {@code Operation} received from another replica to the local replica of a CRDT.
     *
     * @param srcNodeId the id of the node that broadcast the operation
     * @param op the operation to be applied locally
     * @return true if the operation was successfully applied
     */
    protected abstract boolean update(int srcNodeId, Operation<?> op);

    protected void addFinishedNode(int nodeID) {
        finishedNodes.add(nodeID);

        if(finishedNodes.size() == noOfReplicas - 1) {
            allNodesFinished = true;
        }
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    protected void ready() {
        // This needs to be sent to all nodes, as we do not know yet which ones will have replicas of the CRDT
        runtimeManager.send("Running_" + crdtId, 0, runtimeManager.getRemoteNodeIDs());
        //runningNodes.add(nodeId);
    }

    private void buffer(Operation op) {
        buffer.add(op);
    }

    private boolean broadcastBuffer() {
        boolean sent = false;
        Operation op = buffer.poll();

        while(op != null) {
            runtimeManager.send("Operation_" + crdtId, op, ArrayUtils.toPrimitive(runningNodes.toArray(new Integer[0])));
            sent = true;
            op = buffer.poll();
        }

        return sent;
    }
}
