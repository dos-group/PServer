package de.tuberlin.pserver.matrix.crdt;

import de.tuberlin.pserver.crdt.operations.Operation;
import de.tuberlin.pserver.matrix.AbstractReplicatedMatrix;
import de.tuberlin.pserver.runtime.driver.ProgramContext;
import de.tuberlin.pserver.runtime.events.MsgEventHandler;

public abstract class AbstractCRDTMatrix<T> extends AbstractReplicatedMatrix<T> {

    protected AbstractCRDTMatrix(String id, int noOfReplicas, ProgramContext programContext) {
        super(id, noOfReplicas, programContext);

        runtimeManager.addMsgEventListener("Operation_" + id, new MsgEventHandler() {
            @Override
            public void handleMsg(int srcNodeID, Object value) {
                if (value instanceof Operation) {
                    //Suppress the unchecked warning cause by generics cast from object to Operation<T>
                    @SuppressWarnings("unchecked")
                    Operation<?> op = (Operation<?>) value;
                    if (op.getType() == Operation.END) {
                        addFinishedNode(srcNodeID);
                    } else {
                        update(srcNodeID, op);
                    }
                }
            }
        });

        ready();
    }
}
