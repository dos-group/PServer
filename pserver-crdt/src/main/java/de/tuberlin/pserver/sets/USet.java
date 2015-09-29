package de.tuberlin.pserver.sets;

import de.tuberlin.pserver.crdt.Operation;
import de.tuberlin.pserver.runtime.DataManager;

/**
 * The Unique Set assumes each value inserted into the set is unique. Hence, there is no need for a tombstone set.
 */

public class USet<T> extends AbstractUniqueSet<T> {

    public USet(String id, DataManager dataManager) {
        super(id, dataManager);
        run(dataManager);
    }

    @Override
    protected boolean update(int srcNodeId, Operation<T> op, DataManager dm) {
        SetOperation<T> sop = (SetOperation<T>)op;

        if(sop.getType() == SetOperation.ADD) {
            return add(sop.getValue());
        }
        else if(sop.getType() == SetOperation.REMOVE) {
            return remove(sop.getValue());
        }
        else {
            return false;
        }
    }
}
