package de.tuberlin.pserver.crdt;

import de.tuberlin.pserver.AbstractReplicatedDataType;
import de.tuberlin.pserver.operations.Operation;
import de.tuberlin.pserver.runtime.driver.ProgramContext;
import de.tuberlin.pserver.runtime.events.MsgEventHandler;


// TODO: what about exceptions in general
// TODO: what about when counters reach MAX_INT => exception or keep counting somehow?
// TODO: what if someone uses the same id for two crdts?
// TODO: what if only one node is running?
// TODO: maybe use blocking queues for buffers
// TODO: comments and documentation
// TODO: improve the P2P discovery and termination
// TODO: change constructor to CRDT.newReplica(...) to reflect that it is replicas being dealt with?
// TODO: Javadoc package descriptions
// TODO: have a crdt manager class per node? (for example to prevent two crdts with the same id)

/**
 *<p>
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
public abstract class AbstractCRDT<T> extends AbstractReplicatedDataType<T> implements CRDT<T> {
    // TODO: this variable is just for tests


    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    /**
     * Sole constructor
     *
     * @param id the ID of the CRDT that this replica belongs to
     * @param programContext the {@code RuntimeManager} belonging to this {@code MLProgram}
     * */
    protected AbstractCRDT(String id, int noOfReplicas, ProgramContext programContext) {
        super(id, noOfReplicas, programContext);

        runtimeManager.addMsgEventListener("Operation_" + id, new MsgEventHandler() {
            @Override
            public synchronized void handleMsg(int srcNodeID, Object value) {
                if (value instanceof Operation) {
                    @SuppressWarnings("unchecked")
                    Operation<?> op = (Operation<?>) value;
                    //System.out.println("[" + nodeId + "]" + "received update " + op.getType() + ": " + op.getValue());
                    if (op.getType() == Operation.OpType.END) {
                        //System.out.println("[" + nodeId + "]" + " Received END token: " + op.getValue());
                        addFinishedNode(srcNodeID);
                        synchronized (AbstractCRDT.this) {
                            AbstractCRDT.this.notifyAll();
                        }
                    } else {
                        //if(isEnded) System.out.println("[" + nodeId + "]" + " Late submission: " + op.getValue());
                        update(srcNodeID, op);

                    }
                }
            }
        });
    }
}
