package de.tuberlin.pserver.crdt;

import de.tuberlin.pserver.AbstractReplicatedDataType;
import de.tuberlin.pserver.operations.Operation;
import de.tuberlin.pserver.runtime.driver.ProgramContext;
import de.tuberlin.pserver.runtime.events.MsgEventHandler;

public abstract class AbstractCRDT<T> extends AbstractReplicatedDataType<T> implements CRDT<T> {

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------


    protected AbstractCRDT(String id, int noOfReplicas, ProgramContext programContext) {

        super(id, noOfReplicas, programContext);

        runtimeManager.addMsgEventListener("Operation_" + id, new MsgEventHandler() {

            @Override
            public synchronized void handleMsg(int srcNodeID, Object value) {

                if (value instanceof Operation) {

                    @SuppressWarnings("unchecked")
                    Operation<?> op = (Operation<?>) value;

                    if (op.getType() == Operation.OpType.END) {

                        addFinishedNode(srcNodeID);

                        synchronized (AbstractCRDT.this) {

                            AbstractCRDT.this.notifyAll();

                        }

                    } else {

                        update(srcNodeID, op);

                    }

                }

            }

        });

    }

}
